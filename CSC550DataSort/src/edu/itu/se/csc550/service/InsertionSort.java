package edu.itu.se.csc550.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InsertionSort {
	
	public static Integer [] insertionSort(List<Integer> input){
		Integer [] arr = (Integer []) input.toArray(new Integer[input.size()]);
		int key=0;
		int i=0;
		
		for(int j=1;j<arr.length;j++){
			key = arr[j];
			i = j - 1;
			while(i >= 0 && arr[i] > key ){
				arr[i+1] = arr[i];
				i = i-1;
			}
			arr[i+1] = key;
		}
		System.out.println(" ARR >> "+Arrays.toString(arr));
	
		return arr;
	}
	
	public static void main(String[] args) {		
		List<Integer> input = new ArrayList<Integer>();
		input.add(5);input.add(2);input.add(4);input.add(7);
		input.add(1);input.add(3);input.add(2);input.add(6);
		InsertionSort.insertionSort(input);		
	}

}
