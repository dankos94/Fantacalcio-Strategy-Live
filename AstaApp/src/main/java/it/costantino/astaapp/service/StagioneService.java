package it.costantino.astaapp.service;

import it.costantino.astaapp.service.dto.StagioneDTO;
import java.util.List;
import java.util.Optional;

/**
 * Service Interface for managing {@link it.costantino.astaapp.domain.Stagione}.
 */
public interface StagioneService {
    /**
     * Save a stagione.
     *
     * @param stagioneDTO the entity to save.
     * @return the persisted entity.
     */
    StagioneDTO save(StagioneDTO stagioneDTO);

    /**
     * Updates a stagione.
     *
     * @param stagioneDTO the entity to update.
     * @return the persisted entity.
     */
    StagioneDTO update(StagioneDTO stagioneDTO);

    /**
     * Partially updates a stagione.
     *
     * @param stagioneDTO the entity to update partially.
     * @return the persisted entity.
     */
    Optional<StagioneDTO> partialUpdate(StagioneDTO stagioneDTO);

    /**
     * Get all the stagiones.
     *
     * @return the list of entities.
     */
    List<StagioneDTO> findAll();

    /**
     * Get the "id" stagione.
     *
     * @param id the id of the entity.
     * @return the entity.
     */
    Optional<StagioneDTO> findOne(Long id);

    /**
     * Delete the "id" stagione.
     *
     * @param id the id of the entity.
     */
    void delete(Long id);
}
