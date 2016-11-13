package com.mycompany.hadooptrain;


import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author acastano
 */
public class MapperHBase<T> {

    private Configuration conf = null;
    private HBaseAdmin admin;
    private HTable table;

    Class<T> tipo;

    // ya esta programada
    public boolean TableExists() throws Exception {
        admin = new HBaseAdmin(conf);
        return admin.tableExists(getTableName());
    }

    // ya esta programada
    public String getTableName() throws Exception {

        javax.persistence.Table attTable = tipo.getAnnotation(javax.persistence.Table.class);
        return attTable.catalog();
    }

    // ya esta programada
    public void CreateTable() throws Exception {
        creatTable(getTableName(), getFieldNames());
    }

    public MapperHBase(Class<T> tipo) throws Exception {
        javax.persistence.Entity attEntity = tipo.getAnnotation(javax.persistence.Entity.class);
        if (attEntity == null) {
            throw new Exception(tipo.getName() + " is not notated with javax.persistence.Entity");
        }

        conf = HBaseConfiguration.create();
        try {
            admin = new HBaseAdmin(conf);
        } catch (ZooKeeperConnectionException ex) {
            Logger.getLogger(MapperHBase.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(MapperHBase.class.getName()).log(Level.SEVERE, null, ex);
        }
        this.tipo = tipo;
        table = new HTable(conf, getTableName());

    }

    // ya esta programada
    public String[] getFieldNames() {
        ArrayList<String> columns = new ArrayList<String>();
        for (Field field : tipo.getDeclaredFields()) {
            if (field.getAnnotation(javax.persistence.Column.class) != null) {
                columns.add(field.getName());
            }
        }
        return columns.toArray(new String[0]);

    }

    // ya esta programada
    public String[] getKeyFieldNames() {
        ArrayList<String> keys = new ArrayList<String>();
        for (Field field : tipo.getDeclaredFields()) {
            if (field.getAnnotation(javax.persistence.Column.class) != null
                    && field.getAnnotation(javax.persistence.Id.class) != null) {
                keys.add(field.getName());
            }
        }
        return keys.toArray(new String[0]);

    }

    //  ya esta programada
    public String getKey(T obj) throws IllegalArgumentException, IllegalAccessException {
        ArrayList<String> keys = new ArrayList<String>();
        String key = "";
        for (Field field : tipo.getDeclaredFields()) {
            if (field.getAnnotation(javax.persistence.Column.class) != null
                    && field.getAnnotation(javax.persistence.Id.class) != null) {
                //keys.add(field.getName());
                boolean accessible = field.isAccessible();
                field.setAccessible(true);
                if (field.getType().equals(int.class)) {
                    keys.add(String.valueOf(field.getInt(obj)));
                } else if (field.getType().equals(float.class)) {
                    keys.add(String.valueOf(field.getFloat(obj)));
                } else if (field.getType().equals(String.class)) {
                    keys.add(String.valueOf(field.get(obj)));
                }
                field.setAccessible(accessible);
            }
        }
        for (int i = 0; i < keys.size(); i++) {
            key += keys.get(i);
            if (i != 0) {
                key += "_";
            }
        }
        return key;

    }

    // ya esta programada
    public void delete(String key) throws IOException {
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(key.getBytes());
        list.add(del);
        table.delete(list);
        // System.out.println("del recored " + key + " ok.");
    }

    //  ya esta programada
    public void delete(T obj) throws IllegalArgumentException, IllegalAccessException, IOException {
        delete(getKey(obj));
    }

    public void add_or_update(T obj) throws IllegalArgumentException, IllegalAccessException, IOException, Exception {
        String key = getKey(obj);

        // ArrayList<String> columns = new ArrayList<String>();
        for (Field field : tipo.getDeclaredFields()) {
            if (field.getAnnotation(javax.persistence.Column.class) != null) {
                //columns.add(field.getName());
                boolean accessible = field.isAccessible();
                field.setAccessible(true);

                if (field.getType().equals(int.class)) {
                    addRecord(getTableName(), key, field.getName(), "", String.valueOf(field.getInt(obj)));
                } else if (field.getType().equals(float.class)) {
                    addRecord(getTableName(), key, field.getName(), "", String.valueOf(field.getFloat(obj)));
                } else if (field.getType().equals(String.class)) {
                    addRecord(getTableName(), key, field.getName(), "", String.valueOf(field.get(obj)));
                }
                field.setAccessible(accessible);
            }
        }

    }

    // ya esta programada
    public T getRecord(T obj) throws IllegalArgumentException, IllegalAccessException, IOException, InstantiationException, NoSuchFieldException {
        return getRecord(getKey(obj));
    }

    public T getRecord(String key) throws IOException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        T res = tipo.newInstance();
        Get get = new Get(key.getBytes());
        Result rs = table.get(get);
        for (KeyValue kv : rs.raw()) {
            key = new String(kv.getRow());
            String column = new String(kv.getFamily()  );
            String valor = new String(kv.getValue());
            Field field = tipo.getDeclaredField(column);
            boolean accessible = field.isAccessible();
            field.setAccessible(true);

            if (field.getType().equals(int.class)) {
                field.setInt(res, Integer.parseInt(valor));
            } else if (field.getType().equals(float.class)) {
                field.setFloat(res, Float.parseFloat(valor));
            } else if (field.getType().equals(String.class)) {
                field.set(res, valor);
            }

            field.setAccessible(accessible);
        }

        return res;
    }

    public ArrayList<T> getAll() throws Exception {
        ArrayList<T> res = new ArrayList<T>();
        try {

            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for (Result r : ss) {
                String key = "";
                T obj = tipo.newInstance();
                for (KeyValue kv : r.raw()) {
                    String column = new String(kv.getFamily());
                    String valor = new String(kv.getValue());
                    key = new String(kv.getRow());
                    Field field = tipo.getDeclaredField(column);
                    boolean accessible = field.isAccessible();
                    field.setAccessible(true);

                    if (field.getType().equals(int.class)) {
                        field.setInt(obj, Integer.parseInt(valor));
                    } else if (field.getType().equals(float.class)) {
                        field.setFloat(obj, Float.parseFloat(valor));
                    } else if (field.getType().equals(String.class)) {
                        field.set(obj, valor);
                    }

                    field.setAccessible(accessible);
                }
                res.add(obj);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return res;

    }

    public static void main(String[] args) {
        try {
            MapperHBase<Persona> mapper = new MapperHBase<Persona>(Persona.class);

            Persona p = new Persona("1234567890", "Adiel", 32);
            
            if(!mapper.TableExists())
                mapper.CreateTable();
            
            mapper.add_or_update(p);

            System.out.println("------------------------------------------");
            
            ArrayList<Persona> todos = mapper.getAll();
            for (Persona pt : todos) {
                System.out.println(pt);
            }

            System.out.println("------------------------------------------");
            p.setNombre("Adiel2");
            mapper.add_or_update(p);

            for (Persona pt : mapper.getAll()) {
                System.out.println(pt);
            }
            
            Persona p2 = mapper.getRecord("1234567890");
            System.out.println(p2);

            System.out.println("------------------------------------------");
            mapper.delete(p);

            for (Persona pt : mapper.getAll()) {
                System.out.println(pt);
            }
//        try {
//            System.out.println(mapper.TableExists());
//        } catch (IOException ex) {
//            Logger.getLogger(MapperHBase.class.getName()).log(Level.SEVERE, null, ex);
//        }
        } catch (Exception ex) {
            Logger.getLogger(MapperHBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    //  utileria
    public void creatTable(String tableName, String[] familys)
            throws Exception {

        //admin = new HBaseAdmin(conf);
//        if (admin.tableExists(tableName)) {
//            System.out.println("table already exists!");
//        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
//        }
    }

    public void deleteTable(String tableName) throws Exception {
        try {
            admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    public void addRecord(String tableName, String rowKey,
            String family, String qualifier, String value) throws Exception {
        try {
            // table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
