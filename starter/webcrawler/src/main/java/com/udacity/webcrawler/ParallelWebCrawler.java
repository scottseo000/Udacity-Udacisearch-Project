package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final PageParserFactory parserFactory;
  private final Duration timeout;
  private final int popularWordCount;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;
  private final ForkJoinPool pool;
  public static ReentrantLock lock = new ReentrantLock();

  @Inject
  ParallelWebCrawler(
      Clock clock,
      PageParserFactory parserFactory,
      @Timeout Duration timeout,
      @PopularWordCount int popularWordCount,
      @MaxDepth int maxDepth,
      @IgnoredUrls List<Pattern> ignoredUrls,
      @TargetParallelism int threadCount) {
    this.clock = clock;
    this.parserFactory = parserFactory;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    //Logic of ParallelWebCrawler.crawl() is very similar to SequentialWebCrawler.crawl()
    //Started by copying from SequentialWebCrawler.crawl()

    Instant deadline = clock.instant().plus(timeout);

    //Map and Set replaced with ConcurrentMap and ConcurrentSkipListSet, respectively
    ConcurrentMap<String, Integer> counts = new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();
    for (String url : startingUrls) {
      //crawlInternal(url, deadline, maxDepth, counts, visitedUrls);
      //Above line converted into pool.invoke
      pool.invoke(new CrawlInternalParallelTask(url, deadline, maxDepth, counts, visitedUrls));
    }

    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }

    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

  //Creating a class to adapt SequentialWebCrawler.crawlInternal into a class that extends RecursiveTask
  public class CrawlInternalParallelTask extends RecursiveAction {
    private String url;
    private Instant deadline;
    private int maxDepth;
    private ConcurrentMap<String, Integer> counts;
    private ConcurrentSkipListSet<String> visitedUrls;

    public CrawlInternalParallelTask(String url,
                              Instant deadline,
                              int maxDepth,
                              ConcurrentMap<String, Integer> counts,
                              ConcurrentSkipListSet<String> visitedUrls) {
      this.url = url;
      this.deadline = deadline;
      this.maxDepth = maxDepth;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
    }

    @Override
    protected void compute() {
      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {return;}
      for (Pattern pattern : ignoredUrls) {
        if (pattern.matcher(url).matches()) {return;}
      }
      //Need to lock functions to update visitedUrls just in case 2 threads update it at the same moment
      //TODO: figure out whether lock should go inside or outside try block
      try {
        lock.lock();
        if (visitedUrls.contains(url)) {return;}
        visitedUrls.add(url);
      } finally {
        lock.unlock();
      }

      PageParser.Result result = parserFactory.get(url).parse();
      for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        counts.compute(e.getKey(), (k,v) -> v == null ? e.getValue() : v + e.getValue());
      }

      //Task/subtask should create more subtasks and invoke them instead of the usual recursion
      List<CrawlInternalParallelTask> subtasks = new ArrayList<>();
      for (String link : result.getLinks()) {
        subtasks.add(new CrawlInternalParallelTask(link, deadline, maxDepth - 1, counts, visitedUrls));
      }
      invokeAll(subtasks);
      return;
    }
  }
}
