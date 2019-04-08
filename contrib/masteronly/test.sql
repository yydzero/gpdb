

CREATE OR REPLACE FUNCTION test_forloop(count int) RETURNS int AS $$
DECLARE
BEGIN
    FOR i IN 1..count LOOP
        INSERT INTO t1 VALUES (i);
	END LOOP;
	return count;
END;
$$ LANGUAGE plpgsql;