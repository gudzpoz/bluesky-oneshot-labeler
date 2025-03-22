package server

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/config"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gofiber/fiber/v2"
)

var writeToCsvLock = sync.Mutex{}

func (s *FiberServer) CreateReportHandler(c *fiber.Ctx) error {
	auth := c.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return c.Status(fiber.StatusUnauthorized).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidToken",
			Message: "Missing Bearer token",
		})
	}
	bearer := strings.TrimPrefix(auth, "Bearer ")
	ident, err := at_utils.VerifyJwtToken(c.Context(), bearer)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidToken",
			Message: err.Error(),
		})
	}
	if !slices.Contains(config.ModeratorHandles, ident.Handle.String()) {
		return c.Status(fiber.StatusUnauthorized).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidToken",
			Message: "Not a moderator",
		})
	}

	input := atproto.ModerationCreateReport_Input{}
	if err := c.BodyParser(&input); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "BadRequest",
			Message: err.Error(),
		})
	}
	if input.Subject == nil || input.Subject.RepoStrongRef == nil {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "BadRequest",
			Message: "Missing subject",
		})
	}

	var offender syntax.DID
	uri, err := syntax.ParseATURI(input.Subject.RepoStrongRef.Uri)
	if err != nil {
		if strings.HasPrefix(input.Subject.RepoStrongRef.Uri, "did:") {
			offender, err = syntax.ParseDID(input.Subject.RepoStrongRef.Uri)
		}
	} else {
		offender, err = uri.Authority().AsDID()
	}
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "BadRequest",
			Message: err.Error(),
		})
	}

	if strings.Contains(offender.String(), `"`) {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "BadRequest",
			Message: "Invalid character in uri did",
		})
	}

	go s.writeToBlockListCsv(offender.String(), input.ReasonType, input.Reason)

	return c.JSON(&atproto.ModerationCreateReport_Output{
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
		Id:         0,
		Reason:     input.Reason,
		ReasonType: input.ReasonType,
		ReportedBy: ident.DID.String(),
		Subject: &atproto.ModerationCreateReport_Output_Subject{
			RepoStrongRef: input.Subject.RepoStrongRef,
		},
	})
}

func escapeCsvString(s *string) string {
	if s == nil {
		return ""
	}
	return `"` + strings.ReplaceAll(*s, `"`, `""`) + `"`
}

func (s *FiberServer) writeToBlockListCsv(did string, reasonType, reason *string) {
	writeToCsvLock.Lock()
	defer writeToCsvLock.Unlock()

	path := config.ExternalBlockList
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		s.log.Error("failed to open blocklist csv file for writing", "err", err)
		return
	}
	defer f.Close()

	typeStr := escapeCsvString(reasonType)
	line := did + "," + typeStr + "," + escapeCsvString(reason) + "\n"

	if _, err := f.WriteString(line); err != nil {
		s.log.Error("failed to write to blocklist csv file", "err", err)
		return
	}
	if err := f.Sync(); err != nil {
		s.log.Error("failed to sync blocklist csv file", "err", err)
		return
	}

	s.log.Info("added to blocklist csv", "did", did, "type", typeStr)
}
