package it.costantino.astaapp.service;

import it.costantino.astaapp.service.dto.SquadraDTO;
import java.util.List;
import java.util.Optional;

/**
 * Service Interface for managing {@link it.costantino.astaapp.domain.Squadra}.
 */
public interface SquadraService {
    /**
     * Save a squadra.
     *
     * @param squadraDTO the entity to save.
     * @return the persisted entity.
     */
    SquadraDTO save(SquadraDTO squadraDTO);

    /**
     * Updates a squadra.
     *
     * @param squadraDTO the entity to update.
     * @return the persisted entity.
     */
    SquadraDTO update(SquadraDTO squadraDTO);

    /**
     * Partially updates a squadra.
     *
     * @param squadraDTO the entity to update partially.
     * @return the persisted entity.
     */
    Optional<SquadraDTO> partialUpdate(SquadraDTO squadraDTO);

    /**
     * Get all the squadras.
     *
     * @return the list of entities.
     */
    List<SquadraDTO> findAll();

    /**
     * Get the "id" squadra.
     *
     * @param id the id of the entity.
     * @return the entity.
     */
    Optional<SquadraDTO> findOne(Long id);

    /**
     * Delete the "id" squadra.
     *
     * @param id the id of the entity.
     */
    void delete(Long id);
}
