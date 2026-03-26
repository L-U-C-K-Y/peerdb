package connmongo

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// objectIDMinForTimestamp creates an ObjectID with the given timestamp and
// all trailing bytes set to 0x00, producing the smallest possible ObjectID
// for that second.
func objectIDMinForTimestamp(t time.Time) bson.ObjectID {
	var oid [12]byte
	binary.BigEndian.PutUint32(oid[0:4], uint32(t.Unix()))
	return oid
}

// objectIDMaxForTimestamp creates an ObjectID with the given timestamp and
// all trailing bytes set to 0xFF, producing the largest possible ObjectID
// for that second.
func objectIDMaxForTimestamp(t time.Time) bson.ObjectID {
	var oid [12]byte
	binary.BigEndian.PutUint32(oid[0:4], uint32(t.Unix()))
	for i := 4; i < 12; i++ {
		oid[i] = 0xFF
	}
	return oid
}

// minMaxPartitions creates partitions by querying only the min and max _id
// from the collection (leveraging the default _id index), then uniformly
// dividing the timestamp range encoded in the ObjectIDs.
func (c *MongoConnector) minMaxPartitions(
	ctx context.Context,
	collection *mongo.Collection,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	fullTablePartition := []*protos.QRepPartition{{
		PartitionId:        utils.FullTablePartitionID,
		Range:              nil,
		FullTablePartition: true,
	}}
	if numPartitions == 1 {
		c.logger.Info("[mongo] using full table partition for single partition")
		return fullTablePartition, nil
	}

	minID, err := findBoundaryObjectID(ctx, collection, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to find min _id: %w", err)
	}
	maxID, err := findBoundaryObjectID(ctx, collection, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to find max _id: %w", err)
	}

	tsMin := minID.Timestamp()
	tsMax := maxID.Timestamp()
	tsRange := tsMax.Unix() - tsMin.Unix()
	if tsRange <= 0 {
		c.logger.Info("[mongo] min/max timestamps are equal, returning single partition")
		return fullTablePartition, nil
	}

	c.logger.Info("[mongo] using min/max ObjectID timestamp partitioning",
		slog.Time("tsMin", tsMin),
		slog.Time("tsMax", tsMax),
		slog.Int64("numPartitions", numPartitions))

	secondsPerPartition := shared.DivCeil(tsRange, numPartitions)
	partitions := make([]*protos.QRepPartition, 0, numPartitions)
	for i := range numPartitions {
		var start, end bson.ObjectID
		if i == 0 {
			start = minID
		} else {
			start = objectIDMinForTimestamp(time.Unix(tsMin.Unix()+secondsPerPartition*i, 0))
		}
		if i == numPartitions-1 {
			end = maxID
		} else {
			end = objectIDMaxForTimestamp(time.Unix(tsMin.Unix()+secondsPerPartition*(i+1)-1, 0))
		}
		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{
				Range: &protos.PartitionRange_ObjectIdRange{
					ObjectIdRange: &protos.ObjectIdPartitionRange{
						Start: start.Hex(),
						End:   end.Hex(),
					},
				},
			},
			FullTablePartition: false,
		})
	}

	return partitions, nil
}

// findBoundaryObjectID returns the min (direction=1) or max (direction=-1) _id
// in the collection. Because _id is always indexed, this is an efficient O(log n) operation.
func findBoundaryObjectID(
	ctx context.Context,
	collection *mongo.Collection,
	direction int,
) (bson.ObjectID, error) {
	findCmd := bson.D{
		{Key: "find", Value: collection.Name()},
		{Key: "sort", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: direction}}},
		{Key: "limit", Value: 1},
		{Key: "projection", Value: bson.D{{Key: DefaultDocumentKeyColumnName, Value: 1}}},
	}

	cursor, err := collection.Database().RunCommandCursor(ctx, findCmd, options.RunCmd())
	if err != nil {
		return bson.NilObjectID, fmt.Errorf("failed to run find command: %w", err)
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		return bson.NilObjectID, fmt.Errorf("collection is empty")
	}
	var doc struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := bson.Unmarshal(cursor.Current, &doc); err != nil {
		return bson.NilObjectID, fmt.Errorf("failed to unmarshal boundary document: %w", err)
	}
	return doc.ID, nil
}

// bucketAutoPartitions uses MongoDB's $bucketAuto aggregation to create partitions.
// This performs a full collection scan, so it is significantly slower on large collections
// but produces well-balanced partitions.
func (c *MongoConnector) bucketAutoPartitions(
	ctx context.Context,
	collection *mongo.Collection,
	watermarkColumn string,
	numPartitions int64,
) ([]*protos.QRepPartition, error) {
	bucketAutoPipeline := []bson.D{
		{
			{Key: "$bucketAuto", Value: bson.D{
				{Key: "groupBy", Value: "$" + watermarkColumn},
				{Key: "buckets", Value: numPartitions},
			}},
		},
	}

	cursor, err := collection.Aggregate(ctx, bucketAutoPipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate for bucket partitions: %w", err)
	}
	defer cursor.Close(ctx)

	partitions := make([]*protos.QRepPartition, 0, numPartitions)
	for cursor.Next(ctx) {
		var bucket struct {
			ID struct {
				Min bson.ObjectID `bson:"min"`
				Max bson.ObjectID `bson:"max"`
			} `bson:"_id"`
		}
		if err := bson.Unmarshal(cursor.Current, &bucket); err != nil {
			return nil, fmt.Errorf("failed to unmarshal bucket: %w", err)
		}

		partitions = append(partitions, &protos.QRepPartition{
			PartitionId: uuid.NewString(),
			Range: &protos.PartitionRange{
				Range: &protos.PartitionRange_ObjectIdRange{
					ObjectIdRange: &protos.ObjectIdPartitionRange{
						Start: bucket.ID.Min.Hex(),
						End:   bucket.ID.Max.Hex(),
					},
				},
			},
			FullTablePartition: false,
		})
	}
	if err := cursor.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			c.logger.Warn("context canceled while performing bucketAuto aggregation")
		} else {
			c.logger.Error("error while performing bucketAuto aggregation",
				slog.String("error", err.Error()))
		}
		return nil, fmt.Errorf("cursor error during bucketAuto aggregation: %w", err)
	}

	return partitions, nil
}
