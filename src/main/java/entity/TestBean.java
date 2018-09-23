package entity;

import lombok.Data;

@Data
public class TestBean {
    private String name;
    private Integer age;
    private Long count;

    private TestBean2 bean2;
}
