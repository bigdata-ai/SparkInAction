package com.lorelib.spark.java.sparksql;

import java.io.Serializable;

/**
 * @author listening
 * @description
 * @date 2018-02-07 20:41
 * @since 1.0
 */
public class Person implements Serializable {
  private int id;
  private String name;
  private int age;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  @Override
  public String toString() {
    return "id: " + id + ", name: " + name + ", age: " + age;
  }
}
