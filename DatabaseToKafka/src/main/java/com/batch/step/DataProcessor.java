
package com.batch.step;

import com.batch.model.Bill;

import org.springframework.batch.item.ItemProcessor;

public class DataProcessor implements ItemProcessor<Bill,Bill> {

	@Override
	public Bill process(Bill usage) {

		Double billAmount = usage.getDataUsage() * .001 + usage.getMinutes() * .01;
		return new Bill(usage.getId(), usage.getFirstName(), usage.getLastName(),
				usage.getDataUsage(), usage.getMinutes(), billAmount);
	}
}
