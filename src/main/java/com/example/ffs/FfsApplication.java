package com.example.ffs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;

@SpringBootApplication
public class FfsApplication {

    @Bean
    RouterFunction<?> routerFunction(MovieHandler mh){
         return route(GET("/movies"),mh::all)
                 .andRoute(GET("/movies/{movieId}"),mh::byId)
                 .andRoute(GET("/movies/{movieId}/events"),mh::events);
    }
	public static void main(String[] args) {
	    SpringApplication.run(FfsApplication.class, args);

	}
}

@Component
 class MovieHandler {
    final FluxFlexService ffs;

    MovieHandler(FluxFlexService ffs) {
        this.ffs = ffs;
    }

    Mono<ServerResponse> events(ServerRequest request){
        return ok()
                .contentType(MediaType.TEXT_EVENT_STREAM)
                .body(ffs.streamStream(request.pathVariable("movieId")),MovieEvent.class);
    }

    Mono<ServerResponse> byId(ServerRequest request){
        return ok().body(ffs.byId(request.pathVariable("movieId")),Movie.class);
    }

    Mono<ServerResponse> all(ServerRequest request){
        return ok().body(ffs.all(),Movie.class);

    }
}

@Service
class FluxFlexService {
	final MovieRepository movieRepository;

	FluxFlexService(MovieRepository movieRepository) {
		this.movieRepository = movieRepository;
	}

	Flux<MovieEvent> streamStream(String movieId){
	    return byId(movieId).flatMapMany(movie -> {
            Flux<MovieEvent> movieEventFlux = Flux.fromStream(Stream.generate(() -> new MovieEvent(movie, new Date())));
            Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));
            return movieEventFlux.zipWith(interval).map(Tuple2::getT1);
        });
	}

	Flux<Movie> all(){
		return movieRepository.findAll();
	}

	Mono<Movie> byId(String id){
		return movieRepository.findById(id);
	}
}


@Log
@Component
class MovieDataCLR implements CommandLineRunner{

    final MovieRepository movieRepository;

    MovieDataCLR(MovieRepository movieRepository) {
        this.movieRepository = movieRepository;
    }

    @Override
    public void run(String... args) throws Exception {
            movieRepository.deleteAll()
                    .subscribe(null,null,() ->
                    Stream.of("Test1","Test2","Test3","Test4")
                    .map(title -> new Movie(title, UUID.randomUUID().toString()))
                    .forEach(movie -> movieRepository.save(movie).subscribe(movie1 -> log.info(movie1.toString()))));
    }
}

interface MovieRepository extends ReactiveMongoRepository<Movie, String>{

}


@Data
@AllArgsConstructor
@NoArgsConstructor
class MovieEvent{
	Movie movie;
	Date when;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document
class Movie{
	String title;
    @Id
    String id;
}