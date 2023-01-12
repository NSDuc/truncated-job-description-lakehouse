from collections import Counter


class TagCheckerCondition:
    @staticmethod
    def not_one_word_ascii(term: str):
        words = term.split(' ')
        return (len(words) == 1) and not words[0].isascii()

    _drop_words = ["", "you", "have", "work"]

    @staticmethod
    def contain_drop_word(term):
        words = term.split(' ')
        for word in words:
            if word in TagCheckerCondition._drop_words:
                return True
        return False

    @staticmethod
    def is_number(term: str):
        return term.isnumeric()


class TagChecker:
    @staticmethod
    def get_validated_tags(path):
        file = open(path, "r+")
        tags = [line.rstrip() for line in file.readlines()]

        duplicated_tags = [k for k, v in Counter(tags).items() if v > 1]
        if duplicated_tags:
            print(f"duplicated_tags {duplicated_tags}")

        tags.sort()
        file.seek(0)
        file.write('\n'.join(tags))
        file.close()
        return tags

    @staticmethod
    def check(terms, path):
        validated_tags = TagChecker.get_validated_tags(path)
        allowed_terms = set()
        dropped_terms = set()
        for term in terms:
            if TagCheckerCondition.not_one_word_ascii(term) or \
                    TagCheckerCondition.contain_drop_word(term) or \
                    TagCheckerCondition.is_number(term):
                continue

            if term in validated_tags:
                allowed_terms.add(term)
            else:
                dropped_terms.add(term)
        return allowed_terms, dropped_terms
