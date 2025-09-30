TIMING;

TIMING START;
TIMING START one_word;
TIMING START more_than_one_word;

TIMING SHOW;

TIMING STOP;

-- verify that selecting a column named TIMING still work
SELECT timing FROM some_table;
