import edu.stanford.nlp.pipeline.StanfordCoreNLP
import spock.lang.Specification
import web.journal.service.SentimentAnalysis

class JournalTest extends Specification
{
    def "NLP is working"() {
        setup:
        def (VERY_BAD, BAD, NEUTRAL, GOOD, VERY_GOOD) = [0..4]

        def props = new Properties()
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref, sentiment")

        def coreNLP = new StanfordCoreNLP(props)
        def analyst = new SentimentAnalysis()
        analyst.setCoreNLP(coreNLP)

        when:
        def sentiment = analyst.sentimentAnalyze("It's a beautiful morning")

        then:
        sentiment.containsKey("a beautiful morning")
        sentiment.get("a beautiful morning").size() == 1
        sentiment.get("a beautiful morning").first().value > NEUTRAL
    }
}