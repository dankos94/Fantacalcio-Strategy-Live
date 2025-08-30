package it.costantino.astaapp.service;

import it.costantino.astaapp.service.dto.LegaDTO;
import java.util.List;
import java.util.Optional;

/**
 * Service Interface for managing {@link it.costantino.astaapp.domain.Lega}.
 */
public interface LegaService {
    /**
     * Save a lega.
     *
     * @param legaDTO the entity to save.
     * @return the persisted entity.
     */
    LegaDTO save(LegaDTO legaDTO);

    /**
     * Updates a lega.
     *
     * @param legaDTO the entity to update.
     * @return the persisted entity.
     */
    LegaDTO update(LegaDTO legaDTO);

    /**
     * Partially updates a lega.
     *
     * @param legaDTO the entity to update partially.
     * @return the persisted entity.
     */
    Optional<LegaDTO> partialUpdate(LegaDTO legaDTO);

    /**
     * Get all the legas.
     *
     * @return the list of entities.
     */
    List<LegaDTO> findAll();

    /**
     * Get the "id" lega.
     *
     * @param id the id of the entity.
     * @return the entity.
     */
    Optional<LegaDTO> findOne(Long id);

    /**
     * Delete the "id" lega.
     *
     * @param id the id of the entity.
     */
    void delete(Long id);
}
