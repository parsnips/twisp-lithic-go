package twisplithic

import (
	"fmt"

	typev1 "buf.build/gen/go/twisp/api/protocolbuffers/go/twisp/type/v1"
	"github.com/cockroachdb/apd/v3"
)

type Amount struct {
	number   apd.Decimal
	currency string
}

func NewAmount(number, currency string) (Amount, error) {
	decimal := apd.Decimal{}
	if _, _, err := decimal.SetString(number); err != nil {
		return Amount{}, fmt.Errorf("invalid amount %q: %w", number, err)
	}
	return Amount{number: decimal, currency: currency}, nil
}

func MustNewAmount(number, currency string) Amount {
	amount, err := NewAmount(number, currency)
	if err != nil {
		panic(err)
	}
	return amount
}

func AmountFromMoney(money *typev1.Money) (Amount, error) {
	if money == nil {
		return NewAmount("0", "USD")
	}

	coeff := apd.NewBigInt(0).SetBytes(money.GetCoefficient())
	if money.GetNegative() {
		coeff.Neg(coeff)
	}

	decimal := apd.NewWithBigInt(coeff, money.GetExponent())
	decimal.Form = apd.Form(money.GetForm())
	return Amount{number: *decimal, currency: money.GetCurrencyCode()}, nil
}

func (a Amount) Number() string {
	return a.number.Text('f')
}

func (a Amount) CurrencyCode() string {
	return a.currency
}

func (a Amount) Add(b Amount) (Amount, error) {
	if err := a.sameCurrency(b); err != nil {
		return Amount{}, err
	}
	var result apd.Decimal
	if _, err := decimalContext(&a.number, &b.number).Add(&result, &a.number, &b.number); err != nil {
		return Amount{}, err
	}
	return Amount{number: result, currency: a.currency}, nil
}

func (a Amount) Sub(b Amount) (Amount, error) {
	if err := a.sameCurrency(b); err != nil {
		return Amount{}, err
	}
	var result apd.Decimal
	if _, err := decimalContext(&a.number, &b.number).Sub(&result, &a.number, &b.number); err != nil {
		return Amount{}, err
	}
	return Amount{number: result, currency: a.currency}, nil
}

func (a Amount) Mul(multiplier string) (Amount, error) {
	var m apd.Decimal
	if _, _, err := m.SetString(multiplier); err != nil {
		return Amount{}, fmt.Errorf("invalid multiplier %q: %w", multiplier, err)
	}
	var result apd.Decimal
	if _, err := decimalContext(&a.number, &m).Mul(&result, &a.number, &m); err != nil {
		return Amount{}, err
	}
	return Amount{number: result, currency: a.currency}, nil
}

func (a Amount) Cmp(b Amount) (int, error) {
	if err := a.sameCurrency(b); err != nil {
		return 0, err
	}
	return a.number.Cmp(&b.number), nil
}

func (a Amount) Equal(b Amount) bool {
	cmp, err := a.Cmp(b)
	return err == nil && cmp == 0
}

func (a Amount) IsNegative() bool {
	return a.number.Negative && !a.number.IsZero()
}

func (a Amount) IsZero() bool {
	return a.number.IsZero()
}

func (a Amount) sameCurrency(b Amount) error {
	if a.currency != b.currency {
		return fmt.Errorf("currency mismatch: %s != %s", a.currency, b.currency)
	}
	return nil
}

func decimalContext(values ...*apd.Decimal) *apd.Context {
	precision := uint32(34)
	for _, value := range values {
		if value == nil {
			continue
		}
		if digits := uint32(len(value.Coeff.String())); digits > precision {
			precision = digits
		}
	}
	ctx := apd.BaseContext.WithPrecision(precision)
	ctx.Rounding = apd.RoundHalfUp
	return ctx
}
