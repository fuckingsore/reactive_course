import java.util.Objects;

import reactor.core.publisher.Flux;

public class Task {

	public static Flux<String> fizzBuzz(Flux<Integer> input) {
		return input
				.map(x -> x % 3 == 0 ? new IndexedWord(x, "Fizz") : new IndexedWord(x, ""))
				.map(indexedWord -> indexedWord.getIndex() % 5 == 0 ? new IndexedWord(indexedWord.getIndex(),
						indexedWord.getWord() + "Buzz") : indexedWord)
				.map(indexedWord -> indexedWord.getWord().isEmpty() ? String.valueOf(indexedWord.getIndex()) : indexedWord.getWord());
	}

	static class IndexedWord {

		private final int    index;
		private final String word;

		public IndexedWord(int index, String word) {
			this.index = index;
			this.word = Objects.requireNonNull(word);
		}

		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (!(o instanceof IndexedWord)) {
				return false;
			}
			final IndexedWord other = (IndexedWord) o;
			if (this.getIndex() != other.getIndex()) {
				return false;
			}
			final Object this$word = this.getWord();
			final Object other$word = other.getWord();
			if (this$word == null ? other$word != null : !this$word.equals(other$word)) {
				return false;
			}
			return true;
		}

		public int getIndex() {
			return this.index;
		}

		public String getWord() {
			return this.word;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			result = result * PRIME + this.getIndex();
			final Object $word = this.getWord();
			result = result * PRIME + ($word == null ? 43 : $word.hashCode());
			return result;
		}

		public String toString() {
			return "IndexedWord(index=" + this.getIndex() + ", word=" + this.getWord() + ")";
		}
	}
}