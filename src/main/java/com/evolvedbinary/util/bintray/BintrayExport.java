package com.evolvedbinary.util.bintray;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class BintrayExport {

    public static void main(final String[] args) throws IOException, InterruptedException {
        final String username = args[0];
        final String apiKey = args[1];
        final Path outputDir = Paths.get(args[2]).normalize().toAbsolutePath();

        final String bintraySubject = "existdb";
        final String bintrayRepo = "releases";
        final String bintrayPackage = "exist";

        if (!Files.exists(outputDir)) {
            throw new IOException("Output dir does not exist: " + outputDir);
        }

        final HttpClient httpClient = HttpClient.newBuilder()
                .authenticator(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, apiKey.toCharArray());
                    }
                })
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .build();

        final JsonFactory jsonFactory = new JsonFactory();

        // get all the versions
        final String versionsJson =
                httpGetJson(httpClient, "https://api.bintray.com/packages/" + bintraySubject + "/" + bintrayRepo + "/" + bintrayPackage);
        Files.write(outputDir.resolve("versions.json"), versionsJson.getBytes(StandardCharsets.UTF_8));
        System.out.println("Wrote versions.json");

        final List<String> versions = extractStringArray(jsonFactory, versionsJson, "versions");
        System.out.println("Processing " + versions.size() + " individual versions...");

        for (final String versionStr : versions) {
            System.out.println("Processing Version: " + versionStr);

            final Path versionDir = outputDir.resolve(versionStr);
            Files.createDirectories(versionDir);

            // get details of a version
            final String versionJson = httpGetJson(httpClient, "https://api.bintray.com/packages/" + bintraySubject + "/" + bintrayRepo + "/" + bintrayPackage + "/versions/" + versionStr);
            Files.write(versionDir.resolve("version.json"), versionJson.getBytes(StandardCharsets.UTF_8));

            // get files for the version
            final String versionFilesJson = httpGetJson(httpClient, "https://api.bintray.com/packages/" + bintraySubject + "/" + bintrayRepo + "/" + bintrayPackage + "/versions/" + versionStr + "/files");
            Files.write(versionDir.resolve("files.json"), versionFilesJson.getBytes(StandardCharsets.UTF_8));

            final List<String> paths = extractStringFields(jsonFactory, versionFilesJson, "path");
            for (final String path : paths) {
                System.out.println("Processing file: " + path);

                // download the file
                System.out.println("Downloading: " + path);
                try (final InputStream is = httpGetBinary(httpClient, "https://dl.bintray.com/" + bintraySubject + "/" + bintrayRepo + "/" + path)) {
                    final Path filePath = versionDir.resolve(path);
                    Files.copy(is, filePath);
                    System.out.println("OK wrote: " + filePath);
                }
            }
        }
    }

    private static InputStream httpGetBinary(final HttpClient httpClient, final String uri) throws IOException, InterruptedException {
        return httpGet(httpClient, "application/octet-stream", uri, HttpResponse.BodyHandlers.ofInputStream());
    }

    private static String httpGetJson(final HttpClient httpClient, final String uri) throws IOException, InterruptedException {
        return httpGet(httpClient, "application/json", uri, HttpResponse.BodyHandlers.ofString());
    }

    private static <T> T httpGet(final HttpClient httpClient, final String acceptMediaType, final String uri, final HttpResponse.BodyHandler<T> responseBodyHandler) throws IOException, InterruptedException {
        final HttpRequest request = HttpRequest.newBuilder()
                .header("Accept", acceptMediaType)
                .uri(URI.create(uri))
                .build();
        final HttpResponse<T> response = httpClient.send(request, responseBodyHandler);

        if (response.statusCode() != 200) {
            throw new IOException("Expected HTTP 200 but received: " + response.statusCode() + ", for: " + uri);
        }

        return response.body();
    }

    private static List<String> extractStringFields(final JsonFactory jsonFactory, final String json, final String stringFieldName) throws IOException {
        final List<String> list = new ArrayList<>();

        try (final JsonParser jsonParser = jsonFactory.createParser(json)) {
            JsonToken token = jsonParser.nextToken();

            if (token != JsonToken.START_OBJECT && token != JsonToken.START_ARRAY) {
                throw new IOException("Expected data to start with an Object or Array");
            }

            while ((token = jsonParser.nextToken()) != null) {
                if (token == JsonToken.FIELD_NAME) {
                    final String fieldName = jsonParser.getCurrentName();
                    if (fieldName.equals(stringFieldName)) {
                        token = jsonParser.nextToken();
                        if (token == JsonToken.VALUE_STRING) {
                            list.add(jsonParser.getText());
                        }
                    }
                }
            }
        }

        return list;
    }

    private static List<String> extractStringArray(final JsonFactory jsonFactory, final String json, final String arrayFieldName) throws IOException {
        try (final JsonParser jsonParser = jsonFactory.createParser(json)) {
            JsonToken token = jsonParser.nextToken();

            if (token != JsonToken.START_OBJECT && token != JsonToken.START_ARRAY) {
                throw new IOException("Expected data to start with an Object or Array");
            }

            while ((token = jsonParser.nextToken()) != null) {
                if (token == JsonToken.FIELD_NAME) {
                    final String fieldName = jsonParser.getCurrentName();
                    if (fieldName.equals(arrayFieldName)) {

                        final List<String> list = new ArrayList<>();

                        token = jsonParser.nextToken();
                        if (token == JsonToken.START_ARRAY) {
                            while ((token = jsonParser.nextToken()) != JsonToken.END_ARRAY) {
                                if (token == JsonToken.VALUE_STRING) {
                                    list.add(jsonParser.getText());
                                }
                            }
                        }

                        // exit after matching first array
                        return list;
                    }
                }
            }
        }

        return Collections.emptyList();
    }
}
