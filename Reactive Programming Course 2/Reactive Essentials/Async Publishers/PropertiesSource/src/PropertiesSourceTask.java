import reactor.core.publisher.Flux;

public class PropertiesSourceTask {

	static Properties settings;

	public static Flux<Object> createSequence() {
		return Flux.fromArray(settings.asList().toArray());
	}
}