package it.costantino.astaapp.service.mapper;

import it.costantino.astaapp.domain.Roster;
import it.costantino.astaapp.domain.Squadra;
import it.costantino.astaapp.service.dto.RosterDTO;
import it.costantino.astaapp.service.dto.SquadraDTO;
import org.mapstruct.*;

/**
 * Mapper for the entity {@link Roster} and its DTO {@link RosterDTO}.
 */
@Mapper(componentModel = "spring")
public interface RosterMapper extends EntityMapper<RosterDTO, Roster> {
    @Mapping(target = "squadra", source = "squadra", qualifiedByName = "squadraId")
    RosterDTO toDto(Roster s);

    @Named("squadraId")
    @BeanMapping(ignoreByDefault = true)
    @Mapping(target = "id", source = "id")
    SquadraDTO toDtoSquadraId(Squadra squadra);
}
