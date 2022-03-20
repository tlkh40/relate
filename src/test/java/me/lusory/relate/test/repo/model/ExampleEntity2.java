package me.lusory.relate.test.repo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.relational.core.mapping.Table;

@Data
@Table("exampleEntity2")
@AllArgsConstructor
@NoArgsConstructor
public class ExampleEntity2 {
    private String test;
}
