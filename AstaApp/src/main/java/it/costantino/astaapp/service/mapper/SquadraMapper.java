package it.costantino.astaapp.service.mapper;

import it.costantino.astaapp.domain.Lega;
import it.costantino.astaapp.domain.Squadra;
import it.costantino.astaapp.service.dto.LegaDTO;
import it.costantino.astaapp.service.dto.SquadraDTO;
import org.mapstruct.*;

/**
 * Mapper for the entity {@link Squadra} and its DTO {@link SquadraDTO}.
 */
@Mapper(componentModel = "spring")
public interface SquadraMapper extends EntityMapper<SquadraDTO, Squadra> {
    @Mapping(target = "lega", source = "lega", qualifiedByName = "legaId")
    SquadraDTO toDto(Squadra s);

    @Named("legaId")
    @BeanMapping(ignoreByDefault = true)
    @Mapping(target = "id", source = "id")
    LegaDTO toDtoLegaId(Lega lega);
}
