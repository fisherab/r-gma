/* Generated By:JavaCC: Do not edit this line. ParserConstants.java */
package org.glite.rgma.server.services.sql.parser;


/**
 * Token literal values and constants.
 * Generated by org.javacc.parser.OtherFilesGen#start()
 */
public interface ParserConstants {

  /** End of File. */
  int EOF = 0;
  /** RegularExpression Id. */
  int K_ALL = 5;
  /** RegularExpression Id. */
  int K_AND = 6;
  /** RegularExpression Id. */
  int K_AS = 7;
  /** RegularExpression Id. */
  int K_ASC = 8;
  /** RegularExpression Id. */
  int K_AUTO_INCREMENT = 9;
  /** RegularExpression Id. */
  int K_AVG = 10;
  /** RegularExpression Id. */
  int K_BETWEEN = 11;
  /** RegularExpression Id. */
  int K_BY = 12;
  /** RegularExpression Id. */
  int K_CHAR = 13;
  /** RegularExpression Id. */
  int K_COMMENT = 14;
  /** RegularExpression Id. */
  int K_COMMIT = 15;
  /** RegularExpression Id. */
  int K_CONNECT = 16;
  /** RegularExpression Id. */
  int K_COUNT = 17;
  /** RegularExpression Id. */
  int K_CREATE = 18;
  /** RegularExpression Id. */
  int K_DATE = 19;
  /** RegularExpression Id. */
  int K_DELETE = 20;
  /** RegularExpression Id. */
  int K_DESC = 21;
  /** RegularExpression Id. */
  int K_DISTINCT = 22;
  /** RegularExpression Id. */
  int K_EXCLUSIVE = 23;
  /** RegularExpression Id. */
  int K_EXISTS = 24;
  /** RegularExpression Id. */
  int K_FOR = 25;
  /** RegularExpression Id. */
  int K_FROM = 26;
  /** RegularExpression Id. */
  int K_GROUP = 27;
  /** RegularExpression Id. */
  int K_HAVING = 28;
  /** RegularExpression Id. */
  int K_IN = 29;
  /** RegularExpression Id. */
  int K_INDEX = 30;
  /** RegularExpression Id. */
  int K_INSERT = 31;
  /** RegularExpression Id. */
  int K_INTEGER = 32;
  /** RegularExpression Id. */
  int K_INTO = 33;
  /** RegularExpression Id. */
  int K_IS = 34;
  /** RegularExpression Id. */
  int K_LIKE = 35;
  /** RegularExpression Id. */
  int K_KEY = 36;
  /** RegularExpression Id. */
  int K_MAX = 37;
  /** RegularExpression Id. */
  int K_MIN = 38;
  /** RegularExpression Id. */
  int K_MODE = 39;
  /** RegularExpression Id. */
  int K_NATURAL = 40;
  /** RegularExpression Id. */
  int K_NOT = 41;
  /** RegularExpression Id. */
  int K_NOWAIT = 42;
  /** RegularExpression Id. */
  int K_NULL = 43;
  /** RegularExpression Id. */
  int K_OF = 44;
  /** RegularExpression Id. */
  int K_ONLY = 45;
  /** RegularExpression Id. */
  int K_OR = 46;
  /** RegularExpression Id. */
  int K_ORDER = 47;
  /** RegularExpression Id. */
  int K_PRIMARY = 48;
  /** RegularExpression Id. */
  int K_QUIT = 49;
  /** RegularExpression Id. */
  int K_READ = 50;
  /** RegularExpression Id. */
  int K_REAL = 51;
  /** RegularExpression Id. */
  int K_ROLLBACK = 52;
  /** RegularExpression Id. */
  int K_ROW = 53;
  /** RegularExpression Id. */
  int K_SELECT = 54;
  /** RegularExpression Id. */
  int K_SET = 55;
  /** RegularExpression Id. */
  int K_SHARE = 56;
  /** RegularExpression Id. */
  int K_START = 57;
  /** RegularExpression Id. */
  int K_SUM = 58;
  /** RegularExpression Id. */
  int K_TABLE = 59;
  /** RegularExpression Id. */
  int K_UNION = 60;
  /** RegularExpression Id. */
  int K_UPDATE = 61;
  /** RegularExpression Id. */
  int K_VALUES = 62;
  /** RegularExpression Id. */
  int K_VARCHAR = 63;
  /** RegularExpression Id. */
  int K_VIEW = 64;
  /** RegularExpression Id. */
  int K_WHERE = 65;
  /** RegularExpression Id. */
  int K_WITH = 66;
  /** RegularExpression Id. */
  int K_WORK = 67;
  /** RegularExpression Id. */
  int K_WRITE = 68;
  /** RegularExpression Id. */
  int K_TIME = 69;
  /** RegularExpression Id. */
  int K_TIMESTAMP = 70;
  /** RegularExpression Id. */
  int K_DOUBLE = 71;
  /** RegularExpression Id. */
  int K_PRECISION = 72;
  /** RegularExpression Id. */
  int K_FULL = 73;
  /** RegularExpression Id. */
  int K_JOIN = 74;
  /** RegularExpression Id. */
  int K_ON = 75;
  /** RegularExpression Id. */
  int K_LEFT = 76;
  /** RegularExpression Id. */
  int K_RIGHT = 77;
  /** RegularExpression Id. */
  int K_OUTER = 78;
  /** RegularExpression Id. */
  int K_INNER = 79;
  /** RegularExpression Id. */
  int K_USING = 80;
  /** RegularExpression Id. */
  int S_NUMBER = 81;
  /** RegularExpression Id. */
  int FLOAT = 82;
  /** RegularExpression Id. */
  int INTEGER = 83;
  /** RegularExpression Id. */
  int DIGIT = 84;
  /** RegularExpression Id. */
  int LINE_COMMENT = 85;
  /** RegularExpression Id. */
  int MULTI_LINE_COMMENT = 86;
  /** RegularExpression Id. */
  int S_IDENTIFIER = 87;
  /** RegularExpression Id. */
  int LETTER = 88;
  /** RegularExpression Id. */
  int SPECIAL_CHARS = 89;
  /** RegularExpression Id. */
  int S_BIND = 90;
  /** RegularExpression Id. */
  int S_CHAR_LITERAL = 91;

  /** Lexical state. */
  int DEFAULT = 0;

  /** Literal token values. */
  String[] tokenImage = {
    "<EOF>",
    "\" \"",
    "\"\\t\"",
    "\"\\r\"",
    "\"\\n\"",
    "\"ALL\"",
    "\"AND\"",
    "\"AS\"",
    "\"ASC\"",
    "\"AUTO_INCREMENT\"",
    "\"AVG\"",
    "\"BETWEEN\"",
    "\"BY\"",
    "\"CHAR\"",
    "\"COMMENT\"",
    "\"COMMIT\"",
    "\"CONNECT\"",
    "\"COUNT\"",
    "\"CREATE\"",
    "\"DATE\"",
    "\"DELETE\"",
    "\"DESC\"",
    "\"DISTINCT\"",
    "\"EXCLUSIVE\"",
    "\"EXISTS\"",
    "\"FOR\"",
    "\"FROM\"",
    "\"GROUP\"",
    "\"HAVING\"",
    "\"IN\"",
    "\"INDEX\"",
    "\"INSERT\"",
    "\"INTEGER\"",
    "\"INTO\"",
    "\"IS\"",
    "\"LIKE\"",
    "\"KEY\"",
    "\"MAX\"",
    "\"MIN\"",
    "\"MODE\"",
    "\"NATURAL\"",
    "\"NOT\"",
    "\"NOWAIT\"",
    "\"NULL\"",
    "\"OF\"",
    "\"ONLY\"",
    "\"OR\"",
    "\"ORDER\"",
    "\"PRIMARY\"",
    "\"QUIT\"",
    "\"READ\"",
    "\"REAL\"",
    "\"ROLLBACK\"",
    "\"ROW\"",
    "\"SELECT\"",
    "\"SET\"",
    "\"SHARE\"",
    "\"START\"",
    "\"SUM\"",
    "\"TABLE\"",
    "\"UNION\"",
    "\"UPDATE\"",
    "\"VALUES\"",
    "\"VARCHAR\"",
    "\"VIEW\"",
    "\"WHERE\"",
    "\"WITH\"",
    "\"WORK\"",
    "\"WRITE\"",
    "\"TIME\"",
    "\"TIMESTAMP\"",
    "\"DOUBLE\"",
    "\"PRECISION\"",
    "\"FULL\"",
    "\"JOIN\"",
    "\"ON\"",
    "\"LEFT\"",
    "\"RIGHT\"",
    "\"OUTER\"",
    "\"INNER\"",
    "\"USING\"",
    "<S_NUMBER>",
    "<FLOAT>",
    "<INTEGER>",
    "<DIGIT>",
    "<LINE_COMMENT>",
    "<MULTI_LINE_COMMENT>",
    "<S_IDENTIFIER>",
    "<LETTER>",
    "<SPECIAL_CHARS>",
    "<S_BIND>",
    "<S_CHAR_LITERAL>",
    "\"(\"",
    "\")\"",
    "\"=\"",
    "\",\"",
    "\";\"",
    "\".\"",
    "\"!=\"",
    "\"#\"",
    "\"<>\"",
    "\">\"",
    "\">=\"",
    "\"<\"",
    "\"<=\"",
    "\"*\"",
    "\"+\"",
    "\"-\"",
    "\"||\"",
    "\"/\"",
    "\"**\"",
  };

}
