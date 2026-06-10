package twisplithic

import (
	"encoding/binary"
	"fmt"

	corev1 "buf.build/gen/go/twisp/api/protocolbuffers/go/twisp/core/v1"
	typev1 "buf.build/gen/go/twisp/api/protocolbuffers/go/twisp/type/v1"
	"github.com/google/uuid"
)

func newUUID(u uuid.UUID) *typev1.UUID {
	return &typev1.UUID{
		Hi: binary.BigEndian.Uint64(u[:8]),
		Lo: binary.BigEndian.Uint64(u[8:]),
	}
}

func uuidFromProto(u *typev1.UUID) uuid.UUID {
	if u == nil {
		return uuid.Nil
	}
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, u.GetHi())
	binary.BigEndian.PutUint64(b[8:], u.GetLo())
	return uuid.Must(uuid.FromBytes(b))
}

func uuidString(u *typev1.UUID) string {
	return uuidFromProto(u).String()
}

func debitOrCreditString(v corev1.DebitOrCredit) string {
	switch v {
	case corev1.DebitOrCredit_DEBIT_OR_CREDIT_DEBIT_UNSPECIFIED:
		return "DEBIT"
	case corev1.DebitOrCredit_DEBIT_OR_CREDIT_CREDIT:
		return "CREDIT"
	default:
		panic(fmt.Errorf("unexpected DebitOrCredit: %v", v))
	}
}
