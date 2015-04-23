package web.journal.service;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.dcoref.CorefChain.CorefMention;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations;
import edu.stanford.nlp.dcoref.Dictionaries.MentionType;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

//@Component
public class SentimentAnalysis
{
    private StanfordCoreNLP coreNLP;

    //@Autowired
    public void setCoreNLP(StanfordCoreNLP coreNLP)
    {
        this.coreNLP = coreNLP;
    }

    private List<Set<CorefMention>> getMentions(Map<Integer, CorefChain> graph)
    {
        return graph.entrySet().stream()
            .filter(node -> !node.getValue().getMentionMap().isEmpty())
            .map(node -> node.getValue().getMentionMap().values().stream()
                            .flatMap(Set::stream)
                            .collect(Collectors.toSet()))
            .collect(Collectors.toList());
    }

    private List<CorefMention> guessTopics(Set<CorefMention> mentions)
    {
        final Predicate<CorefMention> nonPronominal = mention -> mention.mentionType != MentionType.PRONOMINAL;
        final Comparator<CorefMention> byMentionSpanLen = (lhs, rhs) -> lhs.mentionSpan.length() - rhs.mentionSpan.length();

        return mentions.stream()
                .filter(nonPronominal)
                .sorted(byMentionSpanLen)
                .collect(Collectors.toList());
    }

    public Map<String, Set<Map.Entry<String, Integer>>> sentimentAnalyze(String message)
    {
        final Annotation doc = coreNLP.process(message);

        final List<CoreMap> sentences = doc.get(CoreAnnotations.SentencesAnnotation.class);
        final Map<Integer, CorefChain> graph = doc.get(CorefCoreAnnotations.CorefChainAnnotation.class);

        final Predicate<Set<CorefMention>> hasTopics = mentions -> !guessTopics(mentions).isEmpty();

        final Function<CorefMention, Map.Entry<String, Integer>> f = mention -> new AbstractMap.SimpleImmutableEntry<>(sentences.get(mention.sentNum - 1).toString(), RNNCoreAnnotations.getPredictedClass(sentences.get(mention.sentNum - 1).get(SentimentCoreAnnotations.AnnotatedTree.class)));

        return getMentions(graph).stream()
            .filter(hasTopics)
            .collect(Collectors.toMap(
                mentions -> guessTopics(mentions).get(0).mentionSpan,
                mentions -> mentions.stream().map(f).collect(Collectors.toSet())
            ));
    }
}
