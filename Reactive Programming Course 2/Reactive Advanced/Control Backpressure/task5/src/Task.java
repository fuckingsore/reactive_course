import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

public class Task {

	public static void dynamicDemand(Flux<String> source, CountDownLatch countDownOnComplete) {

		source.subscribe(new BaseSubscriber<String>() {
			Long requested = 1L;
			Long counter = 0L;

			@Override
			protected Subscription upstream() {
				return super.upstream();
			}

			@Override
			public boolean isDisposed() {
				return super.isDisposed();
			}

			@Override
			public void dispose() {
				super.dispose();
			}

			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				request(requested);
			}

			@Override
			protected void hookOnNext(String value) {
				counter++;
				if (Objects.equals(requested, counter)){
					requested *= 2;
					counter = 0L;
					request(requested);
				}

			}

			@Override
			protected void hookOnComplete() {
				super.hookOnComplete();
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				super.hookOnError(throwable);
			}

			@Override
			protected void hookOnCancel() {
				super.hookOnCancel();
			}

			@Override
			protected void hookFinally(SignalType type) {
				countDownOnComplete.countDown();
			}

			@Override
			public String toString() {
				return super.toString();
			}

			@Override
			public Context currentContext() {
				return super.currentContext();
			}

			@Override
			public int hashCode() {
				return super.hashCode();
			}

			@Override
			public boolean equals(Object obj) {
				return super.equals(obj);
			}

			@Override
			protected Object clone() throws CloneNotSupportedException {
				return super.clone();
			}

			@Override
			protected void finalize() throws Throwable {
				super.finalize();
			}
		});
	}
}