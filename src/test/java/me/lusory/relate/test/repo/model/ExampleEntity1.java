package me.lusory.relate.test.repo.model;

import lombok.Builder;
import lombok.Data;
import me.lusory.relate.annotations.ForeignKey;
import me.lusory.relate.annotations.GeneratedValue;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Data
@Builder
@Table("exampleEntity1")
public class ExampleEntity1 {
    @Id
    @GeneratedValue
    private long id;
    private String test;

    @ForeignKey
    private ExampleEntity2 other;
}
