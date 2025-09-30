declare
    TYPE tyrcmessage IS RECORD (id number, message varchar2(100));
    TYPE tytbMessages IS TABLE OF tyrcmessage INDEX BY BINARY_INTEGER;

    tbmessages tytbMessages;
    nuidx      number;

begin
    nuidx := tbmessages.first;
    while nuidx is not null loop
        nuidx := tbmessages.next(nuidx);
    end loop;
end;
/
