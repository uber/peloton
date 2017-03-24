package querybuilder

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeleteBuilderToSql(t *testing.T) {
	ts := time.Now().UnixNano()
	b := Delete("").
		From("a").
		Where("b = ?", 1).
		Using("TIMESTAMP ?", ts)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "DELETE FROM a USING TIMESTAMP ? WHERE b = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{ts, 1}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderToSqlErr(t *testing.T) {
	_, _, err := Delete("").ToSQL()
	assert.Error(t, err)

	_, _, _, err = Delete("").ToUql()
	assert.Error(t, err)
}

func TestDeleteBuilderPlaceholders(t *testing.T) {
	b := Delete("test").Where("x = ? AND y = ?", 1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSQL()
	assert.Equal(t, "DELETE FROM test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSQL()
	assert.Equal(t, "DELETE FROM test WHERE x = $1 AND y = $2", sql)
}

func TestDeleteFlags(t *testing.T) {
	assert.Equal(t, false, Delete("").IsCAS())
	assert.Equal(t, DeleteStmtType, Delete("").StmtType())
}

func TestDeleteAccessor(t *testing.T) {
	query := Delete("").From("a").Where("b = ?", 1)
	deleteAccessor := query.GetData()

	wp := deleteAccessor.GetWhereParts()
	assert.Equal(t, 1, len(wp))
	wpStr, wpArgs, err := wp[0].ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "b = ?", wpStr)
	assert.Equal(t, []interface{}{1}, wpArgs)

	assert.Equal(t, "a", deleteAccessor.GetResource())
	assert.Equal(t, 0, len(deleteAccessor.GetColumns()))
}
