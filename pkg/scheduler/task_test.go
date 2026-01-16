package scheduler

import (
	"testing"
	"time"
)

// Test parseTime function with valid inputs
func TestParseTime_Valid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantHour int
		wantMin  int
	}{
		{"midnight", "00:00", 0, 0},
		{"morning", "09:30", 9, 30},
		{"afternoon", "14:45", 14, 45},
		{"late night", "23:59", 23, 59},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTime(tt.input)

			if err != nil {
				t.Errorf("parseTime(%q) unexpected error: %v", tt.input, err)

				return
			}

			if result.Hour() != tt.wantHour {
				t.Errorf("parseTime(%q) hour = %d, want %d", tt.input, result.Hour(), tt.wantHour)
			}

			if result.Minute() != tt.wantMin {
				t.Errorf("parseTime(%q) minute = %d, want %d", tt.input, result.Minute(), tt.wantMin)
			}

			// Verify it's set to today in UTC
			now := time.Now().UTC()

			if result.Year() != now.Year() || result.Month() != now.Month() || result.Day() != now.Day() {
				t.Errorf("parseTime(%q) not set to today's date", tt.input)
			}
		})
	}
}

func TestParseTime_Invalid(t *testing.T) {
	tests := []struct {
		name      string
		timeStr   string
		wantError bool
	}{
		{
			name:      "empty_string",
			timeStr:   "",
			wantError: true,
		},
		{
			name:      "missing_colon",
			timeStr:   "1230",
			wantError: true,
		},
		{
			name:      "hour_out_of_range",
			timeStr:   "24:00",
			wantError: true,
		},
		{
			name:      "negative_hour",
			timeStr:   "-1:00",
			wantError: true,
		},
		{
			name:      "minute_out_of_range",
			timeStr:   "12:60",
			wantError: true,
		},
		{
			name:      "negative_minute",
			timeStr:   "12:-1",
			wantError: true,
		},
		{
			name:      "non_numeric_hour",
			timeStr:   "ab:30",
			wantError: true,
		},
		{
			name:      "non_numeric_minute",
			timeStr:   "12:cd",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseTime(tt.timeStr)

			if tt.wantError && err == nil {
				t.Errorf("parseTime(%q) expected error, got nil", tt.timeStr)
			}

			if !tt.wantError && err != nil {
				t.Errorf("parseTime(%q) unexpected error: %v", tt.timeStr, err)
			}
		})
	}
}

func TestParseWeekday_Valid(t *testing.T) {
	tests := []struct {
		name   string
		dayStr string
		want   time.Weekday
	}{
		{name: "Sunday", dayStr: "Sunday", want: time.Sunday},
		{name: "Monday", dayStr: "Monday", want: time.Monday},
		{name: "Tuesday", dayStr: "Tuesday", want: time.Tuesday},
		{name: "Wednesday", dayStr: "Wednesday", want: time.Wednesday},
		{name: "Thursday", dayStr: "Thursday", want: time.Thursday},
		{name: "Friday", dayStr: "Friday", want: time.Friday},
		{name: "Saturday", dayStr: "Saturday", want: time.Saturday},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseWeekday(tt.dayStr)

			if err != nil {
				t.Errorf("parseWeekday(%q) unexpected error: %v", tt.dayStr, err)
			}

			if got != tt.want {
				t.Errorf("parseWeekday(%q) = %v, want %v", tt.dayStr, got, tt.want)
			}
		})
	}
}

func TestParseWeekday_Invalid(t *testing.T) {
	tests := []string{
		"",
		"monday",   // lowercase
		"MONDAY",   // uppercase
		"Mon",      // abbreviated
		"Mondayy",  // typo
		"NotADay",  // invalid
		"123",      // numeric
		"Tomorrow", // not a weekday
	}

	for _, dayStr := range tests {
		t.Run(dayStr, func(t *testing.T) {
			_, err := parseWeekday(dayStr)

			if err == nil {
				t.Errorf("parseWeekday(%q) expected error, got nil", dayStr)
			}
		})
	}
}

func TestTaskOptions(t *testing.T) {
	t.Run("WithSchedule", func(t *testing.T) {
		task := &RegisteredTask{}
		WithSchedule(Daily)(task)

		if task.Schedule != Daily {
			t.Errorf("WithSchedule(Daily) failed: got %v, want %v", task.Schedule, Daily)
		}
	})

	t.Run("WithTime", func(t *testing.T) {
		task := &RegisteredTask{}
		WithTime("14:30")(task)

		if task.Time != "14:30" {
			t.Errorf("WithTime failed: got %v, want %v", task.Time, "14:30")
		}
	})

	t.Run("WithWeekday", func(t *testing.T) {
		task := &RegisteredTask{}
		WithWeekday("Monday")(task)

		if task.Weekday != "Monday" {
			t.Errorf("WithWeekday failed: got %v, want %v", task.Weekday, "Monday")
		}
	})

	t.Run("WithDay", func(t *testing.T) {
		task := &RegisteredTask{}
		WithDay(15)(task)

		if task.Day != 15 {
			t.Errorf("WithDay failed: got %v, want %v", task.Day, 15)
		}
	})

	t.Run("WithCron", func(t *testing.T) {
		task := &RegisteredTask{}
		WithCron("0 0 * * *")(task)

		if task.CronExpr != "0 0 * * *" {
			t.Errorf("WithCron failed: got %v, want %v", task.CronExpr, "0 0 * * *")
		}

		if task.Schedule != Cron {
			t.Errorf("WithCron should set Schedule to Cron: got %v, want %v", task.Schedule, Cron)
		}
	})

	t.Run("WithoutOverlap", func(t *testing.T) {
		task := &RegisteredTask{}
		WithoutOverlap()(task)

		if !task.WithoutOverlap {
			t.Errorf("WithoutOverlap failed: got %v, want %v", task.WithoutOverlap, true)
		}
	})

	t.Run("WithCritical", func(t *testing.T) {
		task := &RegisteredTask{}
		WithCritical()(task)

		if !task.IsCritical {
			t.Errorf("WithCritical failed: got %v, want %v", task.IsCritical, true)
		}
	})

	t.Run("MultipleOptions", func(t *testing.T) {
		task := &RegisteredTask{
			Name:    "test-task",
			Handler: nil,
		}

		// Apply multiple options
		options := []TaskOption{
			WithSchedule(Weekly),
			WithTime("09:00"),
			WithWeekday("Friday"),
			WithCritical(),
			WithoutOverlap(),
		}

		for _, opt := range options {
			opt(task)
		}

		if task.Schedule != Weekly {
			t.Errorf("Schedule: got %v, want %v", task.Schedule, Weekly)
		}

		if task.Time != "09:00" {
			t.Errorf("Time: got %v, want %v", task.Time, "09:00")
		}

		if task.Weekday != "Friday" {
			t.Errorf("Weekday: got %v, want %v", task.Weekday, "Friday")
		}

		if !task.IsCritical {
			t.Errorf("IsCritical: got %v, want %v", task.IsCritical, true)
		}

		if !task.WithoutOverlap {
			t.Errorf("WithoutOverlap: got %v, want %v", task.WithoutOverlap, true)
		}
	})
}
