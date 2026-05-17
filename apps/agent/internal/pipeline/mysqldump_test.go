package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

func TestParseMysqldumpVersion(t *testing.T) {
	cases := map[string]string{
		"mysqldump  Ver 8.0.36 for Linux on x86_64 (MySQL Community Server - GPL)": "MySQL 8.0.36",
		"mysqldump  Ver 10.6.16-MariaDB":                                           "MySQL 10.6.16-MariaDB",
	}
	for in, want := range cases {
		require.Equal(t, want, parseMysqldumpVersion(in), "input %q", in)
	}
}

func TestIsMysqldumpHeader(t *testing.T) {
	require.True(t, IsMysqldumpHeader([]byte("-- MySQL dump 10.13")))
	require.True(t, IsMysqldumpHeader([]byte("-- MariaDB dump 10.5")))
	require.False(t, IsMysqldumpHeader([]byte("DROP TABLE x;")))
	require.False(t, IsMysqldumpHeader(nil))
}

func TestMysqldump_DumpWritesPayload(t *testing.T) {
	payload := []byte("-- MySQL dump\nINSERT INTO …;\n")
	mock := &mockRunner{
		outputResp: map[string][]byte{"--version": []byte("mysqldump  Ver 8.0.36 for Linux …")},
		streamResp: payload,
	}
	d := &mysqldump{binary: "mysqldump", runner: mock}
	target := &backupv1.Target{
		Type: backupv1.DbType_MYSQL,
		Connection: &backupv1.ConnectionConfig{
			Host: "h", Port: 3306, Database: "db", Username: "u", PasswordSecretRef: "p",
		},
	}
	var buf testWriter
	info, err := d.Dump(context.Background(), target, &buf)
	require.NoError(t, err)
	require.Equal(t, "MySQL 8.0.36", info.EngineVersion)
	require.Equal(t, payload, buf.Bytes())

	// Confirm --password=p inline arg is present.
	require.NotEmpty(t, mock.calls)
	streamCall := mock.calls[0]
	require.Contains(t, streamCall.Args, "--password=p")
}
