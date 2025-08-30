package it.costantino.astaapp.web.rest;

import it.costantino.astaapp.repository.SquadraRepository;
import it.costantino.astaapp.service.SquadraService;
import it.costantino.astaapp.service.dto.SquadraDTO;
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
 * REST controller for managing {@link it.costantino.astaapp.domain.Squadra}.
 */
@RestController
@RequestMapping("/api/squadras")
public class SquadraResource {

    private static final Logger LOG = LoggerFactory.getLogger(SquadraResource.class);

    private static final String ENTITY_NAME = "squadra";

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final SquadraService squadraService;

    private final SquadraRepository squadraRepository;

    public SquadraResource(SquadraService squadraService, SquadraRepository squadraRepository) {
        this.squadraService = squadraService;
        this.squadraRepository = squadraRepository;
    }

    /**
     * {@code POST  /squadras} : Create a new squadra.
     *
     * @param squadraDTO the squadraDTO to create.
     * @return the {@link ResponseEntity} with status {@code 201 (Created)} and with body the new squadraDTO, or with status {@code 400 (Bad Request)} if the squadra has already an ID.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PostMapping("")
    public ResponseEntity<SquadraDTO> createSquadra(@Valid @RequestBody SquadraDTO squadraDTO) throws URISyntaxException {
        LOG.debug("REST request to save Squadra : {}", squadraDTO);
        if (squadraDTO.getId() != null) {
            throw new BadRequestAlertException("A new squadra cannot already have an ID", ENTITY_NAME, "idexists");
        }
        squadraDTO = squadraService.save(squadraDTO);
        return ResponseEntity.created(new URI("/api/squadras/" + squadraDTO.getId()))
            .headers(HeaderUtil.createEntityCreationAlert(applicationName, false, ENTITY_NAME, squadraDTO.getId().toString()))
            .body(squadraDTO);
    }

    /**
     * {@code PUT  /squadras/:id} : Updates an existing squadra.
     *
     * @param id the id of the squadraDTO to save.
     * @param squadraDTO the squadraDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated squadraDTO,
     * or with status {@code 400 (Bad Request)} if the squadraDTO is not valid,
     * or with status {@code 500 (Internal Server Error)} if the squadraDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PutMapping("/{id}")
    public ResponseEntity<SquadraDTO> updateSquadra(
        @PathVariable(value = "id", required = false) final Long id,
        @Valid @RequestBody SquadraDTO squadraDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to update Squadra : {}, {}", id, squadraDTO);
        if (squadraDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, squadraDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!squadraRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        squadraDTO = squadraService.update(squadraDTO);
        return ResponseEntity.ok()
            .headers(HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, squadraDTO.getId().toString()))
            .body(squadraDTO);
    }

    /**
     * {@code PATCH  /squadras/:id} : Partial updates given fields of an existing squadra, field will ignore if it is null
     *
     * @param id the id of the squadraDTO to save.
     * @param squadraDTO the squadraDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated squadraDTO,
     * or with status {@code 400 (Bad Request)} if the squadraDTO is not valid,
     * or with status {@code 404 (Not Found)} if the squadraDTO is not found,
     * or with status {@code 500 (Internal Server Error)} if the squadraDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PatchMapping(value = "/{id}", consumes = { "application/json", "application/merge-patch+json" })
    public ResponseEntity<SquadraDTO> partialUpdateSquadra(
        @PathVariable(value = "id", required = false) final Long id,
        @NotNull @RequestBody SquadraDTO squadraDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to partial update Squadra partially : {}, {}", id, squadraDTO);
        if (squadraDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, squadraDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!squadraRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        Optional<SquadraDTO> result = squadraService.partialUpdate(squadraDTO);

        return ResponseUtil.wrapOrNotFound(
            result,
            HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, squadraDTO.getId().toString())
        );
    }

    /**
     * {@code GET  /squadras} : get all the squadras.
     *
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and the list of squadras in body.
     */
    @GetMapping("")
    public List<SquadraDTO> getAllSquadras() {
        LOG.debug("REST request to get all Squadras");
        return squadraService.findAll();
    }

    /**
     * {@code GET  /squadras/:id} : get the "id" squadra.
     *
     * @param id the id of the squadraDTO to retrieve.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the squadraDTO, or with status {@code 404 (Not Found)}.
     */
    @GetMapping("/{id}")
    public ResponseEntity<SquadraDTO> getSquadra(@PathVariable("id") Long id) {
        LOG.debug("REST request to get Squadra : {}", id);
        Optional<SquadraDTO> squadraDTO = squadraService.findOne(id);
        return ResponseUtil.wrapOrNotFound(squadraDTO);
    }

    /**
     * {@code DELETE  /squadras/:id} : delete the "id" squadra.
     *
     * @param id the id of the squadraDTO to delete.
     * @return the {@link ResponseEntity} with status {@code 204 (NO_CONTENT)}.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteSquadra(@PathVariable("id") Long id) {
        LOG.debug("REST request to delete Squadra : {}", id);
        squadraService.delete(id);
        return ResponseEntity.noContent()
            .headers(HeaderUtil.createEntityDeletionAlert(applicationName, false, ENTITY_NAME, id.toString()))
            .build();
    }
}
