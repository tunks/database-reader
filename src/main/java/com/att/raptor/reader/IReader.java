/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.att.raptor.reader;

/**
 * Reader Interface
 * @author ebrimatunkara
 * @param <T>
 */
public interface IReader<T> {
      public void read(T data);
}
