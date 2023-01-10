from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import matplotlib.image as pltimg


class WordCloudServiceImpl:
    @staticmethod
    def generate_table(frequency):
        frequency = {k: v for k, v in sorted(frequency.items(), key=lambda item: item[1], reverse=True)}
        header = f"|{'Term':15}|{'Count':<5}|"
        print(header)
        print('-' * len(header))

        i = 0
        for term, count in frequency.items():
            i += 1
            print(f"{i:3}|\"{term:15}\"|{count:<5}|")

    @staticmethod
    def generate_word_cloud(frequency=None, text=None, enable_stopwords=False, savefig_path=None):
        stopwords = set(STOPWORDS) if enable_stopwords else None
        # Create and generate a word cloud image:
        wordcloud = WordCloud(background_color="white",
                              width=2500, height=1500,
                              min_font_size=20,
                              stopwords=stopwords)
        if text:
            wordcloud.generate(text)
        else:
            wordcloud.generate_from_frequencies(frequency)
        # Display the generated image:
        plt.imshow(wordcloud, interpolation='bilinear')

        plt.axis("off")
        if savefig_path:
            wordcloud.to_file(savefig_path)
        plt.show()

    @staticmethod
    def open_word_cloud(path):
        image = pltimg.imread(path)
        plt.imshow(image)
        plt.show(block=False)
        plt.pause(20)
        plt.close()
