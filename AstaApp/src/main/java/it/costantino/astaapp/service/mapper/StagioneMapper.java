package it.costantino.astaapp.service.mapper;

import it.costantino.astaapp.domain.Stagione;
import it.costantino.astaapp.service.dto.StagioneDTO;
import org.mapstruct.*;

/**
 * Mapper for the entity {@link Stagione} and its DTO {@link StagioneDTO}.
 */
@Mapper(componentModel = "spring")
public interface StagioneMapper extends EntityMapper<StagioneDTO, Stagione> {}
