package org.apache.hadoop.examples;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class OlympicsRecord implements WritableComparable<OlympicsRecord> {

	private Text name;
	private IntWritable age;
	private Text country;
	private IntWritable year;
	private Text date;
	private Text category;
	private IntWritable gold;
	private IntWritable silver;
	private IntWritable bronze;
	private IntWritable total;

	public OlympicsRecord() {
		set(new Text(), new IntWritable(), new Text(), new IntWritable(), new Text(), new Text(), new IntWritable(), new IntWritable(), new IntWritable(), new IntWritable());
	}

	private void set(Text name, IntWritable age, Text country, IntWritable year, Text date, Text category, IntWritable gold, IntWritable silver, IntWritable bronze, IntWritable total) {

		this.name = name;
		this.age = age;
		this.country = country;
		this.year = year;
		this.date = date;
		this.category = category;
		this.gold = gold;
		this.silver = silver;
		this.bronze = bronze;
		this.total = total;

	}

	@Override
	public void readFields(DataInput in) throws IOException {

		name.readFields(in);
		age.readFields(in);
		country.readFields(in);
		year.readFields(in);
		date.readFields(in);
		category.readFields(in);
		gold.readFields(in);
		silver.readFields(in);
		bronze.readFields(in);
		total.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {

		name.write(out);
		age.write(out);
		country.write(out);
		year.write(out);
		date.write(out);
		category.write(out);
		gold.write(out);
		silver.write(out);
		bronze.write(out);
		total.write(out);

	}

	@Override
	public int hashCode() {

		return name.hashCode() + age.hashCode() + country.hashCode() + year.hashCode() + date.hashCode()
				+ category.hashCode() + gold.hashCode() + silver.hashCode()
				+ bronze.hashCode() + total.hashCode();

	}

	@Override
	public boolean equals(Object o) {

		if (o instanceof OlympicsRecord) {
			OlympicsRecord sr = (OlympicsRecord) o;
			return name.equals(sr.name) && age.equals(sr.age) && country.equals(sr.country) && year.equals(sr.year)
					&& date.equals(sr.date) && category.equals(sr.category)
					&& gold.equals(sr.gold) && silver.equals(sr.silver)
					&& bronze.equals(sr.bronze) && total.equals(sr.total);
		}
		return false;

	}

	@Override
	public String toString() {

		return name + " " + age + " " + country + " " + year + " " + date + " " + category
				+ " " + gold + " " + silver + " " + bronze + " " + total;

	}

	@Override
	public int compareTo(OlympicsRecord sr) {

	
		int cmp = name.compareTo(sr.name);
		if (cmp != 0) {
			return cmp;
		}

	
		cmp = age.compareTo(sr.age);
		if (cmp != 0) {
			return cmp;
		}
		cmp = country.compareTo(sr.country);
		if (cmp != 0) {
			return cmp;
		}
		
		cmp = year.compareTo(sr.year);
		if (cmp != 0) {
			return cmp;
		}
		cmp = date.compareTo(sr.date);
		if (cmp != 0) {
			return cmp;
		}

		cmp = category.compareTo(sr.category);
		if (cmp != 0) {
			return cmp;
		}

		cmp = gold.compareTo(sr.gold);
		if (cmp != 0) {
			return cmp;
		}
		cmp = silver.compareTo(sr.silver);
		if (cmp != 0) {
			return cmp;
		}
		cmp = bronze.compareTo(sr.bronze);
		if (cmp != 0) {
			return cmp;
		}
		return total.compareTo(sr.total);
	}

	public Text getname() {
		return name;
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
	}

	public IntWritable getAge() {
		return age;
	}

	public void setAge(IntWritable age) {
		this.age = age;
	}

	public Text getDate() {
		return date;
	}

	public void setDate(Text date) {
		this.date = date;
	}

	public Text getCategory() {
		return category;
	}

	public void setCategory(Text category) {
		this.category = category;
	}

	public IntWritable getGold() {
		return gold;
	}

	public void setGold(IntWritable gold) {
		this.gold = gold;
	}

	public IntWritable getSilver() {
		return silver;
	}

	public void setSilver(IntWritable silver) {
		this.silver = silver;
	}

	public IntWritable getBronze() {
		return bronze;
	}

	public void setBronze(IntWritable bronze) {
		this.bronze = bronze;
	}

	public IntWritable getTotal() {
		return total;
	}

	public void setTotal(IntWritable total) {
		this.total = total;
	}

	public Text getCountry() {
		return country;
	}

	public void setCountry(Text country) {
		this.country = country;
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	}