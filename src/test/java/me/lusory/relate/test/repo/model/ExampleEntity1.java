package me.lusory.relate.test.repo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import me.lusory.relate.annotations.ForeignKey;
import me.lusory.relate.annotations.ForeignTable;
import me.lusory.relate.annotations.GeneratedValue;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Data
@Builder
@Table("exampleEntity1")
@AllArgsConstructor
@NoArgsConstructor
public class ExampleEntity1 {
    @Id
    @GeneratedValue
    private long id;
    private String test;
}
