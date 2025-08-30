package it.costantino.astaapp.web.rest;

import it.costantino.astaapp.repository.StagioneRepository;
import it.costantino.astaapp.service.StagioneService;
import it.costantino.astaapp.service.dto.StagioneDTO;
import it.costantino.astaapp.web.rest.errors.BadRequestAlertException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tech.jhipster.web.util.HeaderUtil;
import tech.jhipster.web.util.ResponseUtil;

/**
 * REST controller for managing {@link it.costantino.astaapp.domain.Stagione}.
 */
@RestController
@RequestMapping("/api/stagiones")
public class StagioneResource {

    private static final Logger LOG = LoggerFactory.getLogger(StagioneResource.class);

    private static final String ENTITY_NAME = "stagione";

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final StagioneService stagioneService;

    private final StagioneRepository stagioneRepository;

    public StagioneResource(StagioneService stagioneService, StagioneRepository stagioneRepository) {
        this.stagioneService = stagioneService;
        this.stagioneRepository = stagioneRepository;
    }

    /**
     * {@code POST  /stagiones} : Create a new stagione.
     *
     * @param stagioneDTO the stagioneDTO to create.
     * @return the {@link ResponseEntity} with status {@code 201 (Created)} and with body the new stagioneDTO, or with status {@code 400 (Bad Request)} if the stagione has already an ID.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PostMapping("")
    public ResponseEntity<StagioneDTO> createStagione(@Valid @RequestBody StagioneDTO stagioneDTO) throws URISyntaxException {
        LOG.debug("REST request to save Stagione : {}", stagioneDTO);
        if (stagioneDTO.getId() != null) {
            throw new BadRequestAlertException("A new stagione cannot already have an ID", ENTITY_NAME, "idexists");
        }
        stagioneDTO = stagioneService.save(stagioneDTO);
        return ResponseEntity.created(new URI("/api/stagiones/" + stagioneDTO.getId()))
            .headers(HeaderUtil.createEntityCreationAlert(applicationName, false, ENTITY_NAME, stagioneDTO.getId().toString()))
            .body(stagioneDTO);
    }

    /**
     * {@code PUT  /stagiones/:id} : Updates an existing stagione.
     *
     * @param id the id of the stagioneDTO to save.
     * @param stagioneDTO the stagioneDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated stagioneDTO,
     * or with status {@code 400 (Bad Request)} if the stagioneDTO is not valid,
     * or with status {@code 500 (Internal Server Error)} if the stagioneDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PutMapping("/{id}")
    public ResponseEntity<StagioneDTO> updateStagione(
        @PathVariable(value = "id", required = false) final Long id,
        @Valid @RequestBody StagioneDTO stagioneDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to update Stagione : {}, {}", id, stagioneDTO);
        if (stagioneDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, stagioneDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!stagioneRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        stagioneDTO = stagioneService.update(stagioneDTO);
        return ResponseEntity.ok()
            .headers(HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, stagioneDTO.getId().toString()))
            .body(stagioneDTO);
    }

    /**
     * {@code PATCH  /stagiones/:id} : Partial updates given fields of an existing stagione, field will ignore if it is null
     *
     * @param id the id of the stagioneDTO to save.
     * @param stagioneDTO the stagioneDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated stagioneDTO,
     * or with status {@code 400 (Bad Request)} if the stagioneDTO is not valid,
     * or with status {@code 404 (Not Found)} if the stagioneDTO is not found,
     * or with status {@code 500 (Internal Server Error)} if the stagioneDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PatchMapping(value = "/{id}", consumes = { "application/json", "application/merge-patch+json" })
    public ResponseEntity<StagioneDTO> partialUpdateStagione(
        @PathVariable(value = "id", required = false) final Long id,
        @NotNull @RequestBody StagioneDTO stagioneDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to partial update Stagione partially : {}, {}", id, stagioneDTO);
        if (stagioneDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, stagioneDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!stagioneRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        Optional<StagioneDTO> result = stagioneService.partialUpdate(stagioneDTO);

        return ResponseUtil.wrapOrNotFound(
            result,
            HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, stagioneDTO.getId().toString())
        );
    }

    /**
     * {@code GET  /stagiones} : get all the stagiones.
     *
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and the list of stagiones in body.
     */
    @GetMapping("")
    public List<StagioneDTO> getAllStagiones() {
        LOG.debug("REST request to get all Stagiones");
        return stagioneService.findAll();
    }

    /**
     * {@code GET  /stagiones/:id} : get the "id" stagione.
     *
     * @param id the id of the stagioneDTO to retrieve.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the stagioneDTO, or with status {@code 404 (Not Found)}.
     */
    @GetMapping("/{id}")
    public ResponseEntity<StagioneDTO> getStagione(@PathVariable("id") Long id) {
        LOG.debug("REST request to get Stagione : {}", id);
        Optional<StagioneDTO> stagioneDTO = stagioneService.findOne(id);
        return ResponseUtil.wrapOrNotFound(stagioneDTO);
    }

    /**
     * {@code DELETE  /stagiones/:id} : delete the "id" stagione.
     *
     * @param id the id of the stagioneDTO to delete.
     * @return the {@link ResponseEntity} with status {@code 204 (NO_CONTENT)}.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteStagione(@PathVariable("id") Long id) {
        LOG.debug("REST request to delete Stagione : {}", id);
        stagioneService.delete(id);
        return ResponseEntity.noContent()
            .headers(HeaderUtil.createEntityDeletionAlert(applicationName, false, ENTITY_NAME, id.toString()))
            .build();
    }
}
