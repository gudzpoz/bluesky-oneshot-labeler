package server

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/database"
	"bluesky-oneshot-labeler/internal/listener"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/util/labels"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
)

func (s *FiberServer) RegisterFiberRoutes() {
	// Apply CORS middleware
	s.App.Use(cors.New(cors.Config{
		AllowOrigins:     "*",
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS,PATCH",
		AllowHeaders:     "Accept,Authorization,Content-Type",
		AllowCredentials: false, // credentials require explicit origins
		MaxAge:           300,
	}))

	s.App.Get("/", s.HelloWorldHandler)
	s.App.Get("/xrpc/com.atproto.label.queryLabels", s.QueryLabelsHandler)

}

func (s *FiberServer) HelloWorldHandler(c *fiber.Ctx) error {
	resp := fiber.Map{
		"message": "Hello World",
	}

	return c.JSON(resp)
}

func (s *FiberServer) QueryLabelsHandler(c *fiber.Ctx) error {
	input := database.QueryLabelsInput{
		Cursor: 0,
		Limit:  10,
	}
	err := c.QueryParser(&input)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidRequest",
			Message: err.Error(),
		})
	}

	if input.Limit <= 0 || 250 < input.Limit {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidRequest",
			Message: "limit out of range (0 < limit <= 250)",
		})
	}

	src := at_utils.UserDid.String()
	if input.Sources != nil {
		if !slices.Contains(input.Sources, src) {
			return c.JSON(atproto.LabelQueryLabels_Output{
				Labels: []*atproto.LabelDefs_Label{},
			})
		}
	}

	if input.UriPatterns != nil {
		for i, pat := range input.UriPatterns {
			pat = strings.ReplaceAll(pat, "%", "")
			pat = strings.ReplaceAll(pat, "_", "\\_")
			pat = strings.TrimPrefix(pat, "at://did:")
			if strings.HasSuffix(pat, "*") {
				pat = pat[0 : len(pat)-1]
				if strings.Contains(pat, "*") {
					return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
						ErrStr:  "InvalidRequest",
						Message: "only trailing wildcards allowed",
					})
				}
				pat = pat + "%"
			}
			input.UriPatterns[i] = pat
		}
	}

	queried, err := database.Instance().QueryLabels(&input)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(xrpc.XRPCError{
			ErrStr:  "InternalError",
			Message: err.Error(),
		})
	}

	var cursor *string
	if len(queried) > 0 {
		cursorStr := strconv.FormatInt(queried[len(queried)-1].Id, 10)
		cursor = &cursorStr
	} else {
		cursor = nil
	}

	output := atproto.LabelQueryLabels_Output{
		Cursor: cursor,
		Labels: make([]*atproto.LabelDefs_Label, len(queried)),
	}

	for i, l := range queried {
		var val string
		switch l.Kind {
		case listener.LabelPorn:
			val = listener.LabelPornString
		case listener.LabelSexual:
			val = listener.LabelSexualString
		case listener.LabelNudity:
			val = listener.LabelNudityString
		case listener.LabelGraphicMedia:
			val = listener.LabelGraphicMediaString
		case listener.LabelOffender:
			val = "offender"
		default:
			val = "others"
		}
		unsigned := labels.UnsignedLabel{
			Cts: time.UnixMilli(l.Cts).UTC().Format(time.RFC3339),
			Src: src,
			Uri: "at://did:" + l.Did,
			Val: val,
			Ver: &at_utils.AtProtoVersion,
		}
		signed, err := at_utils.SignLabel(&unsigned)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(xrpc.XRPCError{
				ErrStr:  "InternalError",
				Message: err.Error(),
			})
		}
		output.Labels[i] = signed
	}

	return c.JSON(output)
}
