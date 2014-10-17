CREATE TABLE "P_LITERAL" (
	"s" VARCHAR(100),
	PRIMARY KEY("s"),
	"o" VARCHAR(100)
);

CREATE TABLE "P_OBJECT" (
	"s" VARCHAR(100),
	PRIMARY KEY("s"),
	"o" VARCHAR(100)
);

CREATE TABLE "P_STRING" (
	"s" VARCHAR(100),
	PRIMARY KEY("s"),
	"o" VARCHAR(100)
);

CREATE TABLE "P_INTEGER" (
	"s" VARCHAR(100),
	PRIMARY KEY("s"),
	"o" INTEGER
);

CREATE TABLE "P_UNKNOWN" (
	"s" VARCHAR(100),
	PRIMARY KEY("s"),
	"o" VARCHAR(100)
);

INSERT INTO P_LITERAL (s, o)
    VALUES ("http://example/x1", 'string');

INSERT INTO P_STRING ("s", "o")
    VALUES ("http://example/x2", "string");

INSERT INTO P_LITERAL ("s", "o")
    VALUES ("http://example/x3", "string");

INSERT INTO P_UNKNOWN ("s", "o")
    VALUES ("http://example/x4", "lex");

INSERT INTO P_INTEGER ("s", "o")
    VALUES ("http://example/x5", "1234");

INSERT INTO P_OBJECT ("s", "o")
    VALUES ("http://example/x6", "http://example/iri");

INSERT INTO P_OBJECT ("s", "o")
    VALUES ("http://example/x7", "http://example.org/.genid/this-is-a-bnode");