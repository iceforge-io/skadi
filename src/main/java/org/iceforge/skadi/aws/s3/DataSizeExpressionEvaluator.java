package org.iceforge.skadi.aws.s3;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataSizeExpressionEvaluator {
    public static long evaluate(String expression) {
        List<String> tokens = tokenize(expression);
        BigDecimal result = BigDecimal.ONE;
        for (String token : tokens) {
            if (token.equals("*") || token.equals("L")) {
                continue;
            } else if (token.equals("KB")) {
                result = result.multiply(BigDecimal.valueOf(1024L));
            } else if (token.equals("MB")) {
                result = result.multiply(BigDecimal.valueOf(1024L * 1024L));
            } else if (token.equals("GB")) {
                result = result.multiply(BigDecimal.valueOf(1024L * 1024L * 1024L));
            } else if (token.equals("TB")) {
                result = result.multiply(BigDecimal.valueOf(1024L * 1024L * 1024L * 1024L));
            } else if (token.matches("\\d+\\.\\d*|\\d+"))  {
                result = result.multiply(new BigDecimal(token));
            } else {
                throw new NumberFormatException("Invalid token: " + token);
            }
        }
        return result.setScale(0, BigDecimal.ROUND_FLOOR).longValue();
    }

    public static List<String> tokenize(String expression) {
        List<String> tokens = new ArrayList<>();
        if (expression == null || expression.trim().isEmpty()) {
            throw new NumberFormatException("Empty of null expression: " + expression);
        }
        String s = expression.replaceAll("\\s+", "").toUpperCase();

        // Define regex patterns for numbers, operators, and units
        Pattern pattern = Pattern.compile("\\d+\\.\\d*|\\d+|\\*|L|KB|MB|GB|TB");
        Matcher matcher = pattern.matcher(s);

        // Match and collect tokens
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
        // Validate that the concatenation of tokens matches the input
        String concatenatedTokens = String.join("", tokens);
        if (!concatenatedTokens.equals(s)) {
            throw new NumberFormatException("Invalid input: " + expression);
        }
        return tokens;
    }


    public static void main(String[] args) {
        String expression = "1024*1024*1024*10";
        long result = evaluate(expression);
        System.out.println("Result: " + result); // Output: 10737418240
        String[] tokens = {"1KB", "1MB", "1GB", "1TB", "5KB", "5MB", "5GB", "5TB","1.5GB","9L*2L","11*1024L","ABC"};
        for (String token : tokens) {
            System.out.println(token + " = " + evaluate(token) + " bytes");
        }

    }
}