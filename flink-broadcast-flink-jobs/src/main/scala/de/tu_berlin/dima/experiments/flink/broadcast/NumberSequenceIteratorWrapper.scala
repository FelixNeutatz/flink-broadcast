package de.tu_berlin.dima.experiments.flink.broadcast

import org.apache.flink.util.{NumberSequenceIterator, SplittableIterator}

/**
  * Created by carabolic on 01/06/16.
  */
class NumberSequenceIteratorWrapper(numberSequenceIterator: NumberSequenceIterator) extends SplittableIterator[Long] {

  def this(from: Long, to: Long) = this(new NumberSequenceIterator(from, to))

  override def getMaximumNumberOfSplits = numberSequenceIterator.getMaximumNumberOfSplits

  override def split(numPartitions: Int) =
    numberSequenceIterator.split(numPartitions).map(new NumberSequenceIteratorWrapper(_))

  override def next() = numberSequenceIterator.next()

  override def hasNext = numberSequenceIterator.hasNext

}
