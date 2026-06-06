SELECT *
FROM (SELECT id, name, subject, score FROM scores) s1
    PIVOT (
    MAX(score)
    FOR subject IN ('Chinese', 'Math', 'English')
) s2;
