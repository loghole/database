package query

import (
	"bufio"
	"crypto/md5" //nolint:gosec // not need.
	"strings"
	"sync"
	"unicode/utf8"
)

type OperationType string

func (o OperationType) String() string { return string(o) }

func (o OperationType) TablePrefix() string {
	switch o {
	case SelectType, DeleteType:
		return "from"
	case InsertType, UpsertType:
		return "into"
	case UpdateType:
		return "update"
	case CallType:
		return "call"
	case ExecType:
		return "exec"
	case ExecuteType:
		return "execute"
	default:
		return ""
	}
}

const (
	SelectType  OperationType = "select"
	InsertType  OperationType = "insert"
	UpdateType  OperationType = "update"
	DeleteType  OperationType = "delete"
	CallType    OperationType = "call"
	ExecType    OperationType = "exec"
	ExecuteType OperationType = "execute"
	UpsertType  OperationType = "upsert"
)

type Operation struct {
	Type  OperationType
	Table string
}

type Parser struct {
	cache map[[16]byte]Operation
	mu    sync.RWMutex
}

func NewParser() *Parser {
	return &Parser{cache: make(map[[16]byte]Operation)}
}

func (p *Parser) Parse(stmt string) Operation {
	stmt = strings.ReplaceAll(stmt, "\n", " ")
	stmt = strings.ToLower(strings.TrimSpace(stmt))

	hash := md5.Sum([]byte(stmt)) //nolint:gosec // not need.

	if operation, ok := p.cached(hash); ok {
		return operation
	}

	operation := p.parse(stmt)

	p.memorize(hash, operation)

	return operation
}

func (p *Parser) cached(hash [16]byte) (Operation, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	op, ok := p.cache[hash]

	return op, ok
}

func (p *Parser) memorize(hash [16]byte, value Operation) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.cache[hash] = value
}

func (p *Parser) parse(stmt string) Operation {
	operation := Operation{
		Type:  p.parseOperationType(stmt),
		Table: "unknown",
	}

	prefix := operation.Type.TablePrefix()
	if prefix == "" {
		return operation
	}

	idx := strings.Index(stmt, prefix+" ")
	if idx == -1 {
		return operation
	}

	operation.Table = p.readToken(stmt[idx+len(prefix):])

	return operation
}

func (p *Parser) parseOperationType(stmt string) OperationType {
	scan := bufio.NewScanner(strings.NewReader(stmt))
	scan.Split(scanSQLToken)

	for scan.Scan() {
		switch txt := OperationType(scan.Text()); txt {
		case SelectType,
			InsertType,
			UpdateType,
			DeleteType,
			CallType,
			ExecType,
			ExecuteType,
			UpsertType:
			return txt
		}
	}

	return "unknown"
}

func (p *Parser) readToken(s string) string {
	_, token, _ := scanSQLToken([]byte(s), true)

	return string(token)
}

func scanSQLToken(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Skip leading spaces.
	var start int

	for width := 0; start < len(data); start += width {
		var r rune

		r, width = utf8.DecodeRune(data[start:])

		if !isDelimiter(r) {
			break
		}
	}

	// Scan until space, marking end of word.
	for width, i := 0, start; i < len(data); i += width {
		var r rune

		r, width = utf8.DecodeRune(data[i:])

		if isDelimiter(r) {
			return i + width, data[start:i], nil
		}
	}

	// If we're at EOF, we have a final, non-empty, non-terminated word. Return it.
	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}

	// Request more data.
	return start, nil, nil
}

func isDelimiter(r rune) bool {
	// High-valued ones.
	if '\u2000' <= r && r <= '\u200a' {
		return true
	}

	switch r {
	case ' ', '\t', '\n', '\v', '\f', '\r', ';', '(', ')', '.', ',':
		return true
	case '\u0085', '\u00A0':
		return true
	case '\u1680', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000':
		return true
	default:
		return false
	}
}
