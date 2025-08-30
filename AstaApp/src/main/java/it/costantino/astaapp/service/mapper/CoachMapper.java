package it.costantino.astaapp.service.mapper;

import it.costantino.astaapp.domain.Coach;
import it.costantino.astaapp.domain.Squadra;
import it.costantino.astaapp.service.dto.CoachDTO;
import it.costantino.astaapp.service.dto.SquadraDTO;
import org.mapstruct.*;

/**
 * Mapper for the entity {@link Coach} and its DTO {@link CoachDTO}.
 */
@Mapper(componentModel = "spring")
public interface CoachMapper extends EntityMapper<CoachDTO, Coach> {
    @Mapping(target = "squadra", source = "squadra", qualifiedByName = "squadraId")
    CoachDTO toDto(Coach s);

    @Named("squadraId")
    @BeanMapping(ignoreByDefault = true)
    @Mapping(target = "id", source = "id")
    SquadraDTO toDtoSquadraId(Squadra squadra);
}
