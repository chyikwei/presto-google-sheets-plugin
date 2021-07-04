package com.chyikwei.presto.google.sheets;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum SheetsErrorCode
        implements ErrorCodeSupplier
{
    SHEETS_BAD_CREDENTIALS_ERROR(0, EXTERNAL),
    SHEETS_METASTORE_ERROR(1, EXTERNAL),
    SHEETS_UNKNOWN_TABLE_ERROR(2, USER_ERROR),
    SHEETS_TABLE_LOAD_ERROR(3, INTERNAL_ERROR),
    SHEETS_TABLE_EMPTY_ERROR(4, USER_ERROR);

    private final ErrorCode errorCode;

    SheetsErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0508_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
