package entity;

import lombok.Data;

import java.util.Map;

@Data
public class TestBean2 {
    private String name;
    private Map<String, Integer> map;
}
