package com.partha.ex01workingWithSpark.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class IntegerWithSqrtDto implements Serializable{
	
	private Integer number;
	private Double squareRoot;

}
