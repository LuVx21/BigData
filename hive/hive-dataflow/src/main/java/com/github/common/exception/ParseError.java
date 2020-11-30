package com.github.common.exception;

import lombok.Getter;
import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.RecognitionException;

/**
 * @author: Ren, Xie
 * @desc:
 */
@Getter
public class ParseError {
    private final BaseRecognizer       br;
    private final RecognitionException re;
    private final String[]             tokenNames;

    public ParseError(BaseRecognizer br, RecognitionException re, String[] tokenNames) {
        this.br = br;
        this.re = re;
        this.tokenNames = tokenNames;
    }

    String getMessage() {
        return br.getErrorHeader(re) + " " + br.getErrorMessage(re, tokenNames);
    }
}