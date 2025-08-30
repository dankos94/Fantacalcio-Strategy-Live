package it.costantino.astaapp.service.mapper;

import it.costantino.astaapp.domain.Lega;
import it.costantino.astaapp.domain.Stagione;
import it.costantino.astaapp.service.dto.LegaDTO;
import it.costantino.astaapp.service.dto.StagioneDTO;
import org.mapstruct.*;

/**
 * Mapper for the entity {@link Lega} and its DTO {@link LegaDTO}.
 */
@Mapper(componentModel = "spring")
public interface LegaMapper extends EntityMapper<LegaDTO, Lega> {
    @Mapping(target = "stagione", source = "stagione", qualifiedByName = "stagioneId")
    LegaDTO toDto(Lega s);

    @Named("stagioneId")
    @BeanMapping(ignoreByDefault = true)
    @Mapping(target = "id", source = "id")
    StagioneDTO toDtoStagioneId(Stagione stagione);
}
