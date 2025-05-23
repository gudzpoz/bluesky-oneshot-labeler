package server

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/listener"
	"math"
	"strconv"
	"strings"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gofiber/fiber/v2"
)

func FeedUri() string {
	return "at://" + at_utils.UserDid.String() + "/app.bsky.feed.generator/oneshot"
}

func (s *FiberServer) DescribeFeedGeneratorHandler(c *fiber.Ctx) error {
	return c.JSON(bsky.FeedDescribeFeedGenerator_Output{
		Did: at_utils.UserDid.String(),
		Feeds: []*bsky.FeedDescribeFeedGenerator_Feed{
			{
				Uri: FeedUri(),
			},
		},
	})
}

type FeedSkeletonInput struct {
	Cursor int64  `json:"cursor"`
	Limit  int    `json:"limit"`
	Feed   string `json:"feed"`
}

func (s *FiberServer) GetFeedSkeletonHandler(c *fiber.Ctx) error {
	input := FeedSkeletonInput{
		Cursor: math.MaxInt64,
		Limit:  50,
	}
	err := c.QueryParser(&input)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidRequest",
			Message: err.Error(),
		})
	}
	if input.Limit > 100 || input.Limit <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidRequest",
			Message: "Limit must be between 1 and 100",
		})
	}
	if input.Cursor <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidRequest",
			Message: "Cursor must be greater than 0",
		})
	}
	if input.Feed != FeedUri() {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidRequest",
			Message: "Feed must be " + FeedUri(),
		})
	}

	items, err := s.db.GetFeedItems(&input.Cursor, input.Limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(xrpc.XRPCError{
			ErrStr:  "InternalError",
			Message: err.Error(),
		})
	}
	feed := make([]*bsky.FeedDefs_SkeletonFeedPost, 0, len(items))
	for _, uri := range items {
		splits := strings.SplitN(uri, "/", 2)
		did := splits[0]
		compactDid := strings.TrimPrefix(did, "did:")
		if s.blocker.InBlockList(compactDid) != listener.OutOfBlockList {
			continue
		}
		uri = "at://" + did + "/app.bsky.feed.post/" + splits[1]
		item := &bsky.FeedDefs_SkeletonFeedPost{
			Post: uri,
		}
		feed = append(feed, item)
	}

	var pointer *string
	if len(items) != 0 {
		cursorStr := strconv.FormatInt(input.Cursor, 10)
		pointer = &cursorStr
	}
	return c.JSON(&bsky.FeedGetFeedSkeleton_Output{
		Cursor: pointer,
		Feed:   feed,
	})
}
