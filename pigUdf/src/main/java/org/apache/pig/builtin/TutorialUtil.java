/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.pig.builtin;

/**
 *
 * @author myhome
 */
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class TutorialUtil {
  
  /**
   * This function splits a search query string into a set 
   * of non-empty words 
   */
  protected static String[] splitToWords(String query) {
    List<String> res = new LinkedList<String>();
    String[] words = query.split("\\W");
    for (String word : words) {
      if (!word.equals("")) {
        res.add(word);
      }
    }
    return res.toArray(new String[res.size()]);
  }

  /**
   *   This is a simple utility function that make word-level
   * ngrams from a set of words
   * @param words
   * @param ngrams
   * @param size
   */
  protected static void makeNGram(String[] words, Set<String> ngrams, int size) {
    int stop = words.length - size + 1;
    for (int i = 0; i < stop; i++) {
      StringBuilder sb = new StringBuilder();
      for (int j = 0; j < size; j++) {
        sb.append(words[i + j]).append(" ");
      }
      sb.deleteCharAt(sb.length() - 1);
      ngrams.add(sb.toString());
    }
    if (size > 1) {
      makeNGram(words, ngrams, size - 1);
    }
  }
  
}
