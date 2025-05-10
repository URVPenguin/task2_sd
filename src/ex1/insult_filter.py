class InsultFilter:
    def __init__(self):
        self.insults = {"idiot", "stupid", "dumb"}

    def filter_text(self, text):
        for word in self.insults:
            text = text.replace(word, "CENSORED")
        return text