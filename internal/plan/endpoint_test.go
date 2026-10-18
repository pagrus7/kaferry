package plan

import (
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestPlannerRejectsInvalidKafkaTopicEndpoints(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"orders@a@b", "topic endpoint must be topic@alias"},
		{"@default", "invalid topic endpoint"},
		{"orders@", "invalid topic endpoint"},
		{"..", "invalid Kafka topic name"},
		{"bad/name", "invalid Kafka topic name"},
		{strings.Repeat("x", 250), "invalid Kafka topic name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given valid SQL with an invalid destination topic or connection suffix.
			query := "INSERT INTO TOPIC '" + tc.name + "' SELECT * FROM TOPIC 'orders' OPTIONS(GROUP='g', START=EARLIEST)"
			statement, err := sql.NewParser().Parse(query)
			require.NoError(t, err)

			// When validating the copy plan.
			_, err = NewPlanner(environment{}).Build(statement, profiles)

			// Then endpoint validation fails with the specific topic or alias diagnostic.
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}
