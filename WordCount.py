from mrjob.job import MRJob
from mrjob.step import MRStep


class WordCount(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_words,
                   reducer=self.reducer_words),
            MRStep(mapper=self.mapper_output,
                   reducer = self.reducer_output)
        ]
    def mapper_words(self, _, line):
        (userID, words) = line.split(',')
        yield words, 1

    def reducer_words(self, words, occurences):
        yield words, sum(occurences)
        
    def mapper_output(self, words, count):
        yield '%04d'%int(count), words
        
    def reducer_output(self, count, words):
        for word in words:
            yield count, word

if __name__ == '__main__':
    WordCount.run()
