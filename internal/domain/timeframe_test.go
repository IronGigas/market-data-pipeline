package domain_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

func TestTimeframeDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tf   domain.Timeframe
		want time.Duration
	}{
		{name: "1s", tf: domain.TF1s, want: time.Second},
		{name: "1m", tf: domain.TF1m, want: time.Minute},
		{name: "неизвестный таймфрейм даёт ноль", tf: domain.Timeframe("1h"), want: 0},
		{name: "пустой таймфрейм даёт ноль", tf: domain.Timeframe(""), want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.tf.Duration())
		})
	}
}

func TestTimeframeTruncate(t *testing.T) {
	t.Parallel()

	// Часовой пояс с ненулевым смещением, чтобы проверить: Truncate режет
	// окно по абсолютному времени, а не по локальным «часам» входного t.
	msk := time.FixedZone("MSK", 3*60*60)

	tests := []struct {
		name string
		tf   domain.Timeframe
		in   time.Time
		want time.Time
	}{
		{
			name: "1s: середина окна округляется вниз",
			tf:   domain.TF1s,
			in:   time.Date(2026, 9, 3, 10, 15, 30, 123_456_789, time.UTC),
			want: time.Date(2026, 9, 3, 10, 15, 30, 0, time.UTC),
		},
		{
			name: "1s: точная граница не сдвигается",
			tf:   domain.TF1s,
			in:   time.Date(2026, 9, 3, 10, 15, 30, 0, time.UTC),
			want: time.Date(2026, 9, 3, 10, 15, 30, 0, time.UTC),
		},
		{
			name: "1s: последняя наносекунда остаётся в своём окне",
			tf:   domain.TF1s,
			in:   time.Date(2026, 9, 3, 10, 15, 30, 999_999_999, time.UTC),
			want: time.Date(2026, 9, 3, 10, 15, 30, 0, time.UTC),
		},
		{
			name: "1m: середина окна округляется вниз",
			tf:   domain.TF1m,
			in:   time.Date(2026, 9, 3, 10, 15, 30, 123_000_000, time.UTC),
			want: time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC),
		},
		{
			name: "1m: точная граница не сдвигается",
			tf:   domain.TF1m,
			in:   time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC),
			want: time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC),
		},
		{
			name: "1m: последняя наносекунда остаётся в своём окне",
			tf:   domain.TF1m,
			in:   time.Date(2026, 9, 3, 10, 15, 59, 999_999_999, time.UTC),
			want: time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC),
		},
		{
			name: "1m: граница часа",
			tf:   domain.TF1m,
			in:   time.Date(2026, 9, 3, 11, 0, 0, 1, time.UTC),
			want: time.Date(2026, 9, 3, 11, 0, 0, 0, time.UTC),
		},
		{
			name: "1m: граница суток",
			tf:   domain.TF1m,
			in:   time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC),
			want: time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "1s: Unix epoch",
			tf:   domain.TF1s,
			in:   time.Unix(0, 500_000_000).UTC(),
			want: time.Unix(0, 0).UTC(),
		},
		{
			name: "1m: время до epoch округляется вниз, а не к нулю",
			tf:   domain.TF1m,
			in:   time.Date(1969, 12, 31, 23, 58, 30, 0, time.UTC),
			want: time.Date(1969, 12, 31, 23, 58, 0, 0, time.UTC),
		},
		{
			name: "1m: вход в MSK режется по абсолютному времени",
			tf:   domain.TF1m,
			in:   time.Date(2026, 9, 3, 13, 15, 30, 0, msk),
			want: time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC),
		},
		{
			name: "1s: вход в MSK режется по абсолютному времени",
			tf:   domain.TF1s,
			in:   time.Date(2026, 9, 3, 13, 15, 30, 750_000_000, msk),
			want: time.Date(2026, 9, 3, 10, 15, 30, 0, time.UTC),
		},
		{
			name: "неизвестный таймфрейм возвращает исходное время в UTC",
			tf:   domain.Timeframe("1h"),
			in:   time.Date(2026, 9, 3, 13, 15, 30, 123, msk),
			want: time.Date(2026, 9, 3, 10, 15, 30, 123, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.tf.Truncate(tc.in)

			require.True(t, got.Equal(tc.want), "want %s, got %s", tc.want, got)
			require.Equal(t, time.UTC, got.Location(), "Truncate обязан возвращать UTC")
		})
	}
}

func TestParseTimeframe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    domain.Timeframe
		wantErr bool
	}{
		{name: "1s", in: "1s", want: domain.TF1s},
		{name: "1m", in: "1m", want: domain.TF1m},
		{name: "пробелы обрезаются", in: "  1m  ", want: domain.TF1m},
		{name: "регистр не важен", in: "1M", want: domain.TF1m},
		{name: "неподдерживаемый таймфрейм", in: "1h", wantErr: true},
		{name: "пустая строка", in: "", wantErr: true},
		{name: "мусор", in: "minute", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := domain.ParseTimeframe(tc.in)
			if tc.wantErr {
				require.ErrorIs(t, err, domain.ErrUnknownTimeframe)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
