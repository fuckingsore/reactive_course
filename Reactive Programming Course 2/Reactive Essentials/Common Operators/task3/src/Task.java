import reactor.core.publisher.Flux;

public class Task {

	public static Flux<Character> createSequence(Flux<String> stringFlux) {
		return stringFlux.flatMap(text -> Flux.fromArray(text.split(""))).map(character -> character.charAt(0));
	}
}