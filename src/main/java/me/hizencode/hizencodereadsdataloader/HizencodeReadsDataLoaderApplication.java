package me.hizencode.hizencodereadsdataloader;

import me.hizencode.hizencodereadsdataloader.author.Author;
import me.hizencode.hizencodereadsdataloader.author.AuthorRepository;
import me.hizencode.hizencodereadsdataloader.book.Book;
import me.hizencode.hizencodereadsdataloader.book.BookRepository;
import me.hizencode.hizencodereadsdataloader.connection.DataStaxAstraProperties;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class HizencodeReadsDataLoaderApplication {

    @Autowired
    private AuthorRepository authorRepository;

    @Autowired
    private BookRepository bookRepository;

    @Value(value = "${datadump.location.authors}")
    private String authorsDumpLocation;

    @Value(value = "${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(HizencodeReadsDataLoaderApplication.class, args);
    }

    @PostConstruct
    public void start() {
        initAuthors();
        initWorks();
    }

    private void initAuthors() {
        var path = Paths.get(authorsDumpLocation);
        try (var lines = Files.lines(path)) {

            lines.forEach(line -> {
                //Read and parse line
                var jsonString = line.substring(line.indexOf("{"));
                try {
                    var jsonObject = new JSONObject(jsonString);

                    //Create author
                    var author = new Author();
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));

                    //Persist author
                    System.out.println("Saving author with name: " + author.getName());
                    authorRepository.save(author);

                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initWorks() {
        var path = Paths.get(worksDumpLocation);

        try (var lines = Files.lines(path)) {

            lines.forEach(line -> {
                //Read and parse line
                var jsonString = line.substring(line.indexOf("{"));
                try {
                    var jsonObject = new JSONObject(jsonString);

                    //Create book
                    var book = new Book();

                    book.setId(jsonObject.getString("key").replace("/works/", ""));

                    book.setTitle(jsonObject.optString("title"));

                    var description = jsonObject.optJSONObject("description");
                    if (description != null) {
                        book.setDescription(description.optString("value"));
                    }

                    var coversJsonArr = jsonObject.optJSONArray("covers");
                    if (coversJsonArr != null) {
                        var coversIds = new ArrayList<String>();
                        for (int i = 0; i < coversJsonArr.length(); i++) {
                            coversIds.add(coversJsonArr.getString(i));
                        }
                        book.setCoverIds(coversIds);
                    }

                    var authorsJsonArr = jsonObject.optJSONArray("authors");
                    if (authorsJsonArr != null) {

                        var authorIds = new ArrayList<String>();
                        var authorNames = new ArrayList<String>();

                        for (int i = 0; i < authorsJsonArr.length(); i++) {
                            var authorJsonObj = authorsJsonArr.getJSONObject(i).optJSONObject("author");
                            if (authorJsonObj != null) {
                                var authorId = authorJsonObj.optString("key").replace("/authors/", "");

                                authorIds.add(authorId);
                                var optAuthor = authorRepository.findById(authorId);
                                if (optAuthor.isPresent()) {
                                    authorNames.add(optAuthor.get().getName());
                                } else {
                                    authorNames.add("Unknown author");
                                }
                            }
                        }

                        book.setAuthorIds(authorIds);
                        book.setAuthorNames(authorNames);
                    }

                    var created = jsonObject.optJSONObject("created");
                    if (created != null) {
                        var dateStr = created.optString("value");
                        book.setPublishedDate(LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                    }

                    //Persist book
                    System.out.println("Saving book with title: " + book.getTitle());
                    bookRepository.save(book);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
