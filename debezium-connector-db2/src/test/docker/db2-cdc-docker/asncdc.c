#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqludf.h>
#include <sqlstate.h>

void SQL_API_FN asncdcservice(
    SQLUDF_VARCHAR *asnCommand, /* input */
    SQLUDF_VARCHAR *asnService,
    SQLUDF_CLOB *fileData, /* output */
    /* null indicators */
    SQLUDF_NULLIND *asnCommand_ind, /* input */
    SQLUDF_NULLIND *asnService_ind,
    SQLUDF_NULLIND *fileData_ind,
    SQLUDF_TRAIL_ARGS,
    struct sqludf_dbinfo *dbinfo)
{

    int fd;
    char tmpFileName[] = "/tmp/fileXXXXXX";
    fd = mkstemp(tmpFileName);

    int strcheck = 0;
    char cmdstring[256];


    char* szDb2path = getenv("HOME");

   

    char str[20];
    int len = 0;
    char c;
    char *buffer = NULL;
    FILE *pidfile;

    char dbname[129];
    memset(dbname, '\0', 129);
    strncpy(dbname, (char *)(dbinfo->dbname), dbinfo->dbnamelen);
    dbname[dbinfo->dbnamelen] = '\0';

    int pid;
    if (strcmp(asnService, "asncdc") == 0)
    {
        strcheck = sprintf(cmdstring, "pgrep -fx \"%s/sqllib/bin/asncap capture_schema=%s capture_server=%s\" > %s", szDb2path, asnService, dbname, tmpFileName);
        int callcheck;
        callcheck = system(cmdstring);
        pidfile = fopen(tmpFileName, "r");
        while ((c = fgetc(pidfile)) != EOF)
        {
            if (c == '\n')
            {
                break;
            }
            len++;
        }
        buffer = (char *)malloc(sizeof(char) * len);
        fseek(pidfile, 0, SEEK_SET);
        fread(buffer, sizeof(char), len, pidfile);
        fclose(pidfile);
        pidfile = fopen(tmpFileName, "w");
        if (strcmp(asnCommand, "start") == 0)
        {
            if (len == 0) // is not running
            {
                strcheck = sprintf(cmdstring, "%s/sqllib/bin/asncap capture_schema=%s capture_server=%s &", szDb2path, asnService, dbname);
                fprintf(pidfile, "start -->  %s \n", cmdstring);
                callcheck = system(cmdstring);
            }
            else
            {
                fprintf(pidfile, "asncap is already running");
            }
        }
        if ((strcmp(asnCommand, "prune") == 0) ||
            (strcmp(asnCommand, "reinit") == 0) ||
            (strcmp(asnCommand, "suspend") == 0) ||
            (strcmp(asnCommand, "resume") == 0) ||
            (strcmp(asnCommand, "status") == 0) ||
            (strcmp(asnCommand, "stop") == 0))
        {
            if (len > 0)
            {
                //buffer[len] = '\0';
                //strcheck = sprintf(cmdstring, "/bin/kill -SIGINT %s   ", buffer);
                //fprintf(pidfile, "stop -->  %s", cmdstring);
                //callcheck = system(cmdstring);
                strcheck = sprintf(cmdstring, "%s/sqllib/bin/asnccmd capture_schema=%s capture_server=%s %s >> %s", szDb2path, asnService, dbname, asnCommand, tmpFileName);
                //fprintf(pidfile, "%s -->  %s \n", cmdstring, asnCommand);
                callcheck = system(cmdstring);
            }
            else
            {
                fprintf(pidfile, "asncap is not running");
            }
        }

        fclose(pidfile);
    }
    /* system(cmdstring); */

    int rc = 0;
    long fileSize = 0;
    size_t readCnt = 0;
    FILE *f = NULL;

    f = fopen(tmpFileName, "r");
    if (!f)
    {
        strcpy(SQLUDF_MSGTX, "Could not open file ");
        strncat(SQLUDF_MSGTX, tmpFileName,
                SQLUDF_MSGTEXT_LEN - strlen(SQLUDF_MSGTX) - 1);
        strncpy(SQLUDF_STATE, "38100", SQLUDF_SQLSTATE_LEN);
        return;
    }

    rc = fseek(f, 0, SEEK_END);
    if (rc)
    {
        sprintf(SQLUDF_MSGTX, "fseek() failed with rc = %d", rc);
        strncpy(SQLUDF_STATE, "38101", SQLUDF_SQLSTATE_LEN);
        return;
    }

    /* verify the file size */
    fileSize = ftell(f);
    if (fileSize > fileData->length)
    {
        strcpy(SQLUDF_MSGTX, "File too large");
        strncpy(SQLUDF_STATE, "38102", SQLUDF_SQLSTATE_LEN);
        return;
    }

    /* go to the beginning and read the entire file */
    rc = fseek(f, 0, 0);
    if (rc)
    {
        sprintf(SQLUDF_MSGTX, "fseek() failed with rc = %d", rc);
        strncpy(SQLUDF_STATE, "38103", SQLUDF_SQLSTATE_LEN);
        return;
    }

    readCnt = fread(fileData->data, 1, fileSize, f);
    if (readCnt != fileSize)
    {
        /* raise a warning that something weird is going on */
        sprintf(SQLUDF_MSGTX, "Could not read entire file "
                              "(%d vs %d)",
                readCnt, fileSize);
        strncpy(SQLUDF_STATE, "01H10", SQLUDF_SQLSTATE_LEN);
        *fileData_ind = -1;
    }
    else
    {
        fileData->length = readCnt;
        *fileData_ind = 0;
    }
    // remove temorary file
    rc = remove(tmpFileName);
    //fclose(pFile);
}
