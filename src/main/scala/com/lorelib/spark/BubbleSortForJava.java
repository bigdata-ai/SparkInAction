package com.lorelib.spark;

/**
 * @author listening
 * @description BubbleSortForJava:
 * @create 2018 01 13 下午3:23.
 */

public class BubbleSortForJava {
  public static void main(String[] args) {
    int[] arr = new int[]{19, 3, 20, 13, 99, 88, 55, 35, 46};

    //sorted(arr);
    sortedTopN(arr, 5);

    print(arr);
  }

  private static void sorted(int[] arr) {
    for (int i = 0; i < arr.length; i++) {
      for (int j = arr.length - 1; j > i; j--) {
        if (arr[i] > arr[j]) {
          int tmp = arr[i];
          arr[i] = arr[j];
          arr[j] = tmp;
        }
      }
    }
  }

  private static void sortedTopN(int[] arr, int topN) {
    int[] topNArr = new int[topN];
    for (int i = 0; i < arr.length; i++) {
      int v = arr[i];
      for (int j = 0; j < topN; j++) {
        if (topNArr[j] == 0) {
          topNArr[j] = v;
          break;
        } else if (v > topNArr[j]) {
          for (int k = topN - 1; k > j; k--) {
            topNArr[k] = topNArr[k - 1];
          }
          topNArr[j] = v;
          break;
        }
      }
    }
    print(topNArr);
    System.out.println("\n#########");
  }

  private static void print(int[] arr) {
    for (int i = 0; i < arr.length; i++) {
      System.out.print(arr[i] + "  ");
    }
  }
}
